# Les composants et leur rôle

Six briques exécutables : deux Lambda et quatre jobs Glue. Chacune lit une zone,
en écrit une autre. Le partage est simple — **les Lambda traitent des messages,
les jobs Glue traitent des fichiers.**

```
   ① API catalogue                      ② dépôts partenaires
         │                                       │
   [λ producer]                          landing/partners/
         │  messages                             │  fichiers
        SQS                                      │
         │                                       │
   [λ consumer] ──────> bronze/events/ <──── [Glue 1]
                              │
                        [Glue 2] ──> silver/events/ + quality/
                              │
                        [Glue 3] ──> gold/ (6 tables)
                              │
                        [Glue 4] ──> PostgreSQL
```

---

## λ 1 · `ecommerce_producer` — fabrique les événements

**Crée des messages SQS. Rien dans S3.**

| | |
|---|---|
| **Lit** | un catalogue produits : liste inline `PRODUCTS`, CSV/JSON sur S3, ou l'API `ECOMMERCE_API_URL` |
| **Écrit** | des messages dans `QUEUE_URL`, par lots de 10 (`SendMessageBatch`) |
| **Déclenchée par** | Step Functions, premier pas de la chaîne |

Elle **simule des sessions d'achat** complètes : un visiteur cherche, consulte,
met au panier, retire, passe commande, échoue un paiement, se fait rembourser.
C'est ce qui donne aux tables funnel, sessions et RFM quelque chose de réel à
calculer — sans ça, une seule vue produit par article ne produit aucun entonnoir.

Chaque enregistrement passe les règles qualité **avant** la file, et porte déjà
sa `idempotency_key`. Une source en échec (l'API injoignable) est ignorée, jamais
fatale.

Elle ne boucle pas : un lot, puis elle rend la main. La cadence appartient à la
machine d'état.

**Ce qu'elle exige** : `CONFIG_PATH` en variable d'environnement ou dans
l'événement — sinon `RuntimeError: CONFIG_PATH not provided`.
Pour le mode sessions : `"SIMULATION": {"ENABLED": true, "SESSIONS": 50}`.

---

## λ 2 · `stream_processor` — pose les événements dans le lac

**C'est elle qui crée les premiers objets S3.**

| | |
|---|---|
| **Lit** | les messages SQS |
| **Écrit** | `bronze/events/dt=…/hour=…/*.json` (NDJSON) et `quarantine/events/…` |
| **Déclenchée par** | l'*event source mapping* SQS, à mesure que les messages arrivent |

Un objet par partition, partitionné sur l'heure **de l'événement** et non du
traitement : un message en retard ou rejoué reste dans l'heure à laquelle il
appartient. Les enregistrements qui échouent la validation vont en quarantaine
**avec le nom des règles violées** — diagnosticables et rejouables, plutôt que
disparus dans une ligne de log.

Sur échec d'écriture, elle renvoie les `messageId` concernés
(`ReportBatchItemFailures`) : seuls ceux-là sont réessayés, pas le lot entier.

---

## Glue 1 · `glue_landing_ingest` — les fichiers partenaires

| | |
|---|---|
| **Lit** | `landing/partners/` — `.csv`, `.json`, `.ndjson`, `.jsonl` |
| **Écrit** | `bronze/events/` · `quarantine/landing/` · déplace les fichiers vers `landing/_processed/` |

Un partenaire exporte des **colonnes**, pas des événements : une ligne CSV porte
`product_id` à plat, alors que tout l'aval cherche `product.product_id`. Ce job
relève chaque ligne dans le format v3 — blocs imbriqués, arithmétique du panier,
et **la même clé d'idempotence** que la voie streaming. Un fichier déposé et un
message en file sont donc le même enregistrement une fois en silver.

*Pourquoi un job et pas une Lambda* : un dépôt est un **lot**, qui peut faire dix
millions de lignes — ça ne tient pas dans une fonction plafonnée à 15 minutes.

Les fichiers traités sont **déplacés**, pour que la run suivante ne les relise
pas. Piège classique : déposer dans `landing/` au lieu de `landing/partners/` —
le job réussit sans rien écrire.

---

## Glue 2 · `glue_bronze_to_silver` — nettoie et déduplique

| | |
|---|---|
| **Lit** | `bronze/events/` |
| **Écrit** | `silver/events/` (Parquet, 41 colonnes typées) · `quality/` (un rapport par run) |

Aplatit les blocs imbriqués, type les colonnes, calcule les dérivées (montant
net, remise, catégorie de prix), et **déduplique sur `idempotency_key`** — c'est
ici que les doublons des deux sources se referment.

Écrit en Parquet partitionné `partition_date` / `partition_hour`, coalescé pour
éviter les milliers de petits fichiers qui rendent Athena lent et cher.

Produit aussi le **rapport qualité** du lot : rétention, doublons, taux de nuls.
Avec `FAIL_ON_QUALITY`, un dépassement de seuil interrompt le job — et donc la
chaîne, ce qui laisse gold et l'entrepôt sur les chiffres d'hier plutôt que sur
les mauvais d'aujourd'hui.

---

## Glue 3 · `glue_silver_to_gold` — les tables analytiques

| | |
|---|---|
| **Lit** | `silver/events/` |
| **Écrit** | six jeux sous `gold/` |

| Table | Une ligne par | Répond à |
|---|---|---|
| `sessions` | session de navigation | combien de rebonds, quelle durée |
| `funnel_daily` | jour | où les visiteurs abandonnent |
| `orders` | commande | panier moyen, annulations, remboursements |
| `customer_rfm` | client | récence / fréquence / montant, segment |
| `product_daily` | produit × jour | ce qui se vend, classé par revenu |
| `anomalies` | événement suspect | montant aberrant, achat anonyme, remise extrême |

`GOLD_DATASETS` absent construit les six ; une liste explicite n'en reconstruit
qu'une partie — utile pour rejouer une seule table après correction.

---

## Glue 4 · `glue_rds_load` — publie dans l'entrepôt

| | |
|---|---|
| **Lit** | `silver/events/` + les six tables `gold/` |
| **Écrit** | sept tables PostgreSQL, via Spark JDBC |

Sept cibles en une passe, une seule connexion. `fact_events` en `append` (c'est
un journal), les six autres en `overwrite`.

L'`overwrite` fait un **TRUNCATE**, pas un DROP : types, index et droits
survivent à chaque chargement. C'est aussi pourquoi **les tables doivent exister
avant** — `sql/warehouse/001_schema.sql`. Le job écrit dedans, il ne les crée
pas.

La connexion vient entièrement du fichier `--CONFIG_PATH` : `RDS_HOST`,
`RDS_PORT`, `RDS_DATABASE`, `RDS_SCHEMA`, `RDS_USERNAME`, `RDS_PASSWORD`,
`RDS_SSLMODE`. Ou `RDS_SECRET_ARN`, pour que le mot de passe ne soit pas écrit
dans un fichier posé sur S3.

---

## Qui écrit quoi, en une table

| Zone S3 | Écrite par | Lue par |
|---|---|---|
| `landing/partners/` | **vous** (dépôt manuel ou partenaire) | Glue 1 |
| `bronze/events/` | λ consumer **et** Glue 1 | Glue 2 |
| `silver/events/` | Glue 2 | Glue 3, Glue 4 |
| `gold/*` | Glue 3 | Glue 4 |
| `quarantine/` | λ consumer, Glue 1 | vous, pour diagnostic |
| `quality/` | Glue 2 | vous, pour surveillance |

Une zone vide bloque tout ce qui la suit. C'est pourquoi chaque job dit
maintenant, dans ses logs, où il a cherché et ce qu'il a trouvé — voir
[`DEPLOIEMENT.md`](DEPLOIEMENT.md).
