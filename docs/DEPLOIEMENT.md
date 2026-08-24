# Faire tourner le pipeline

Ordre à respecter : chaque étage lit ce que le précédent a écrit. Un job qui
trouve sa source vide ne plante pas — il sort en succès sans rien produire, ce
qui a exactement l'allure d'un run réussi. C'est le piège principal.

Les valeurs ci-dessous supposent le lac `data-lake-abjx4fbmp43a8f6o` ; remplacez
par le vôtre partout.

---

## 0 · Le fichier de config, une fois pour les quatre jobs

Partez de [`config/pipeline.example.json`](../config/pipeline.example.json) :
un seul fichier, lu par les quatre jobs. Les préfixes `LANDING_` / `RDS_` /
`GOLD_` ne se chevauchent pas, donc chaque job y prend ce qui le concerne et
ignore le reste.

```bash
export LAKE=data-lake-abjx4fbmp43a8f6o
export CONFIG_BUCKET=tfstate-e-commerce-owrfil3c

# remplacez OUTPUT_BUCKET, puis déposez
aws s3 cp config/pipeline.example.json s3://$CONFIG_BUCKET/pipeline.json
```

Deux choses à **ne pas** mettre :

- **`JOB_NAME`** — Glue passe `--JOB_NAME` à chaque exécution et c'est lui qui
  nomme les métriques. Le figer dans un fichier partagé ferait signer les quatre
  jobs du même nom.
- **`--DEPS_PATH` / `--extra-py-files`** — chaque script Glue est autonome. Un
  `dependencies.zip` contenant `boto3` écrase la version native de Glue et
  provoque `DataNotFoundError: Unable to load data for: endpoints`.

Arguments de chaque job Glue, au complet :

```
--CONFIG_PATH   s3://tfstate-e-commerce-owrfil3c/pipeline.json
```

C'est tout. `--JOB_NAME` est injecté par Glue.

---

## 1 · Les données de départ

Le job d'ingestion lit **`landing/partners/`**, pas `landing/`. C'est la cause
la plus fréquente d'un run « Succeeded » qui n'écrit rien.

```bash
aws s3 cp data/catalog/ s3://$LAKE/catalog/ --recursive
aws s3 cp data/landing/partner_events.csv    s3://$LAKE/landing/partners/
aws s3 cp data/landing/partner_events.ndjson s3://$LAKE/landing/partners/

# vérification : les deux fichiers doivent apparaître
aws s3 ls s3://$LAKE/landing/partners/
```

Extensions lues : `.csv`, `.json`, `.ndjson`, `.jsonl`. Tout le reste est ignoré
(et désormais journalisé comme tel).

---

## 2 · L'entrepôt, avant le premier chargement

Le job 4 écrit dans des tables existantes — il ne les crée pas. C'est voulu :
un `overwrite` fait un TRUNCATE, ce qui préserve types, index et droits.

```bash
export RDS_URL="postgresql://adminuser:MOTDEPASSE@VOTRE-HOST:5432/ecommerce?sslmode=require"

psql "$RDS_URL" -f sql/warehouse/001_schema.sql   # 7 tables + index + rôles
psql "$RDS_URL" -f sql/warehouse/002_views.sql
psql "$RDS_URL" -f sql/warehouse/003_upsert.sql
```

Puis remplissez `RDS_HOST` et `RDS_PASSWORD` dans `pipeline.json` — ou, mieux,
laissez-les vides et posez `RDS_SECRET_ARN` : un mot de passe écrit dans un
fichier sur S3 est lisible par tout ce qui a `s3:GetObject` sur le bucket.

---

## 3 · Les jobs, dans l'ordre

| # | Job Glue | lit | écrit |
|---|---|---|---|
| 1 | `glue_landing_ingest` | `landing/partners/` | `bronze/events/` |
| 2 | `glue_bronze_to_silver` | `bronze/events/` | `silver/events/` + `quality/` |
| 3 | `glue_silver_to_gold` | `silver/events/` | `gold/` (6 tables) |
| 4 | `glue_rds_load` | `silver/` + `gold/` | PostgreSQL |

Après **chaque** job, vérifiez que la zone suivante existe :

```bash
aws s3 ls s3://$LAKE/bronze/events/    # après le job 1
aws s3 ls s3://$LAKE/silver/events/    # après le job 2
aws s3 ls s3://$LAKE/gold/             # après le job 3
```

Une sortie vide veut dire que le job n'a rien produit, même s'il affiche
« Succeeded ». Ouvrez alors ses *Output logs* CloudWatch : chaque job dit
maintenant où il a cherché et ce qu'il a trouvé.

---

## Diagnostiquer un run « Succeeded » qui n'écrit rien

| Ce que dit le log | Ce qu'il faut faire |
|---|---|
| `No file to ingest in s3://…/landing/partners/` + `Found under s3://…/landing/ instead: …` | Les fichiers sont un préfixe trop haut. Déplacez-les dans `landing/partners/`. |
| `Drop zone … — 0 to ingest, N ignored (extension)` | Mauvaise extension. Renommez en `.csv` / `.ndjson`. |
| `Nothing to process: s3://…/bronze/events/ does not exist yet` | Le job 1 n'a rien écrit. Reprenez à l'étape 1. |
| `Missing RDS settings: [...]` | Le message nomme le fichier lu et les clés à y ajouter. |

Pour que ces situations **échouent** au lieu de réussir à vide — utile une fois
la chaîne stabilisée, pour que Step Functions s'arrête plutôt que de propager du
vide :

```json
"FAIL_ON_EMPTY_DROP_ZONE": true,
"FAIL_ON_EMPTY_BRONZE": true
```

Je les laisse à `false` par défaut : dans une chaîne quotidienne, un jour sans
dépôt partenaire est légitime et ne doit pas casser l'exécution.

---

## Vérifier de bout en bout

```sql
SELECT count(*) FROM analytics.fact_events;
SELECT * FROM analytics.v_revenue_daily ORDER BY 1 DESC LIMIT 7;
```

Si `fact_events` est peuplée, les quatre étages ont fonctionné.
