-- =============================================================================
-- LE LAC VU PAR ATHENA — requêtes de contrôle.
--
--   Base            : ecommerce_lake        (config/orchestration/triggers.json)
--   Catalogué par   : ecommerce-lake-crawler
--   Cible du crawler: s3://<lake>/silver/  ->  events
--                     s3://<lake>/gold/    ->  sessions, orders, funnel_daily,
--                                              customer_rfm, product_daily,
--                                              anomalies
--
-- bronze/ n'est PAS catalogué : c'est du brut, son schéma a le droit de dériver.
--
-- CONVENTION — les §2 à §5 sont des contrôles : **zéro ligne = tout va bien**.
-- Une ligne renvoyée est un invariant du pipeline qui vient d'être violé.
--
-- COÛT — Athena facture les octets lus. `events` est partitionné par
-- (partition_date, partition_hour), les tables gold par partition_date.
-- Filtrer sur partition_date dans CHAQUE requête : sans ça un contrôle sur un
-- jour lit tout l'historique. Chaque requête ci-dessous porte soit une date
-- littérale '2026-08-25' à remplacer par la vôtre, soit une fenêtre glissante
-- de 7 ou 30 jours.
-- =============================================================================


-- ─────────────────────────────────────────────
-- 0. LE CATALOGUE EST-IL À JOUR
-- ─────────────────────────────────────────────

-- Ce que le crawler a réellement créé. Les noms ci-dessous supposent un nom de
-- table = nom du dossier ; vérifier avant de lancer le reste.
SHOW TABLES IN ecommerce_lake;

DESCRIBE ecommerce_lake.events;

-- Les partitions connues d'Athena. Une partition écrite par Glue mais absente
-- ici = le crawler n'est pas repassé, et toutes les requêtes qui suivent
-- l'ignorent silencieusement.
SHOW PARTITIONS ecommerce_lake.events;

-- Enregistrer les partitions sans attendre le crawler.
MSCK REPAIR TABLE ecommerce_lake.events;

-- Premier coup d'œil, borné à une partition pour ne rien payer.
SELECT *
FROM ecommerce_lake.events
WHERE partition_date = '2026-08-25'
LIMIT 20;


-- ─────────────────────────────────────────────
-- 1. VOLUMÉTRIE
-- ─────────────────────────────────────────────

-- Le volume par jour et par heure. Un trou dans les heures = un run de
-- landing_ingest ou de bronze_to_silver qui n'a pas tourné.
SELECT
    partition_date,
    partition_hour,
    count(*)                       AS events,
    approx_distinct(session_id)    AS sessions,
    approx_distinct(customer_id)   AS customers,
    approx_distinct(order_id)      AS orders
FROM ecommerce_lake.events
WHERE partition_date >= cast(current_date - interval '7' day as varchar)
GROUP BY partition_date, partition_hour
ORDER BY partition_date DESC, partition_hour DESC;

-- Répartition des types d'événements sur un jour. Un type qui disparaît d'un
-- coup est un changement de contrat côté producteur.
SELECT
    event_type,
    count(*)                                   AS events,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM ecommerce_lake.events
WHERE partition_date = '2026-08-25'
GROUP BY event_type
ORDER BY events DESC;


-- ─────────────────────────────────────────────
-- 2. INVARIANTS DE LA COUCHE SILVER
--    (zéro ligne attendue)
-- ─────────────────────────────────────────────

-- 2.1 — Unicité de idempotency_key.
-- C'est l'invariant sur lequel repose l'index unique de analytics.fact_events :
-- un doublon ici fait échouer le chargement RDS, ou double le chiffre
-- d'affaires si l'index a été retiré.
SELECT idempotency_key, count(*) AS occurrences
FROM ecommerce_lake.events
WHERE partition_date = '2026-08-25'
  AND idempotency_key IS NOT NULL
GROUP BY idempotency_key
HAVING count(*) > 1
ORDER BY occurrences DESC
LIMIT 50;

-- 2.2 — Champs obligatoires. bronze_to_silver est censé avoir rejeté en
-- quarantaine tout ce qui en manque.
SELECT
    count(*) FILTER (WHERE event_id IS NULL)       AS null_event_id,
    count(*) FILTER (WHERE event_type IS NULL)     AS null_event_type,
    count(*) FILTER (WHERE occurred_ts IS NULL)    AS null_occurred_ts,
    count(*) FILTER (WHERE session_id IS NULL)     AS null_session_id,
    count(*) FILTER (WHERE idempotency_key IS NULL) AS null_idempotency_key
FROM ecommerce_lake.events
WHERE partition_date = '2026-08-25'
HAVING count(*) FILTER (WHERE event_id IS NULL) > 0
    OR count(*) FILTER (WHERE event_type IS NULL) > 0
    OR count(*) FILTER (WHERE occurred_ts IS NULL) > 0
    OR count(*) FILTER (WHERE idempotency_key IS NULL) > 0;

-- 2.3 — La partition dit-elle la vérité ?
-- partition_date doit être dérivé de occurred_ts. Un décalage veut dire qu'une
-- requête filtrant sur la partition rate des événements de ce jour-là.
SELECT
    partition_date,
    date_format(occurred_ts, '%Y-%m-%d') AS date_reelle,
    count(*)                                   AS evenements
FROM ecommerce_lake.events
WHERE partition_date = '2026-08-25'
  AND date_format(occurred_ts, '%Y-%m-%d') <> partition_date
GROUP BY partition_date, date_format(occurred_ts, '%Y-%m-%d')
ORDER BY evenements DESC;

-- Idem pour l'heure.
SELECT partition_hour, date_format(occurred_ts, '%H') AS heure_reelle, count(*) AS evenements
FROM ecommerce_lake.events
WHERE partition_date = '2026-08-25'
  AND date_format(occurred_ts, '%H') <> partition_hour
GROUP BY partition_hour, date_format(occurred_ts, '%H');

-- 2.4 — La convention de signe.
-- signed_net_amount est négatif pour les annulations et remboursements, positif
-- ailleurs. C'est ce qui permet un SUM sans CASE partout en aval : si la règle
-- casse ici, tous les revenus calculés en aval sont faux.
SELECT event_type, signed_net_amount, net_amount, count(*) AS occurrences
FROM ecommerce_lake.events
WHERE partition_date = '2026-08-25'
  AND (
        (event_type IN ('order_cancelled', 'refund_issued') AND signed_net_amount > 0)
     OR (event_type NOT IN ('order_cancelled', 'refund_issued') AND signed_net_amount < 0)
      )
GROUP BY event_type, signed_net_amount, net_amount
LIMIT 50;

-- 2.5 — Cohérence arithmétique des montants d'une commande.
-- net = gross - remise, à un centime près (arrondis décimaux).
SELECT event_id, gross_amount, discount_amount, net_amount,
       gross_amount - discount_amount - net_amount AS ecart
FROM ecommerce_lake.events
WHERE partition_date = '2026-08-25'
  AND event_type = 'order_placed'
  AND abs(gross_amount - discount_amount - net_amount) > 0.01
LIMIT 50;

-- 2.6 — Valeurs hors domaine.
SELECT event_id, event_type, quantity, discount_pct, product_price
FROM ecommerce_lake.events
WHERE partition_date = '2026-08-25'
  AND (quantity < 0 OR discount_pct < 0 OR discount_pct > 100 OR product_price < 0)
LIMIT 50;


-- ─────────────────────────────────────────────
-- 3. RÉCONCILIATION SILVER ↔ GOLD
--    Ce que le job silver_to_gold prétend avoir agrégé.
--    (zéro ligne attendue)
-- ─────────────────────────────────────────────

-- 3.1 — Revenu : funnel_daily.revenue = SUM(signed_net_amount) du silver,
-- par (jour, canal). Le job filtre session_id IS NOT NULL — le contrôle doit
-- appliquer exactement le même filtre, sinon il crée un faux écart.
WITH silver AS (
    SELECT partition_date, channel, sum(signed_net_amount) AS revenue
    FROM ecommerce_lake.events
    WHERE partition_date = '2026-08-25'
      AND session_id IS NOT NULL
    GROUP BY partition_date, channel
),
gold AS (
    SELECT partition_date, channel, revenue
    FROM ecommerce_lake.funnel_daily
    WHERE partition_date = '2026-08-25'
)
SELECT
    coalesce(s.partition_date, g.partition_date) AS partition_date,
    coalesce(s.channel, g.channel)               AS channel,
    s.revenue                                    AS revenu_silver,
    g.revenue                                    AS revenu_gold,
    coalesce(s.revenue, 0) - coalesce(g.revenue, 0) AS ecart
FROM silver s
FULL OUTER JOIN gold g
  ON s.partition_date = g.partition_date AND s.channel = g.channel
WHERE abs(coalesce(s.revenue, 0) - coalesce(g.revenue, 0)) > 0.01;

-- 3.2 — Sessions : funnel_daily.sessions = sessions distinctes du silver.
WITH silver AS (
    SELECT partition_date, channel, count(DISTINCT session_id) AS sessions
    FROM ecommerce_lake.events
    WHERE partition_date = '2026-08-25'
      AND session_id IS NOT NULL
    GROUP BY partition_date, channel
)
SELECT s.channel, s.sessions AS sessions_silver, g.sessions AS sessions_gold
FROM silver s
JOIN ecommerce_lake.funnel_daily g
  ON g.partition_date = s.partition_date AND g.channel = s.channel
WHERE g.partition_date = '2026-08-25'
  AND s.sessions <> g.sessions;

-- 3.3 — Commandes : une ligne de gold.orders par order_id du silver.
WITH silver AS (
    SELECT DISTINCT order_id
    FROM ecommerce_lake.events
    WHERE partition_date = '2026-08-25'
      AND event_type = 'order_placed'
      AND order_id IS NOT NULL
)
SELECT s.order_id
FROM silver s
LEFT JOIN ecommerce_lake.orders o
       ON o.order_id = s.order_id AND o.partition_date = '2026-08-25'
WHERE o.order_id IS NULL
LIMIT 50;

-- 3.4 — Sessions gold : pas de session inventée, pas de session perdue.
SELECT
    count(*) FILTER (WHERE e.session_id IS NULL) AS sessions_sans_evenement,
    count(*) FILTER (WHERE g.session_id IS NULL) AS sessions_non_agregees
FROM (SELECT DISTINCT session_id FROM ecommerce_lake.sessions   WHERE partition_date = '2026-08-25') g
FULL OUTER JOIN
     (SELECT DISTINCT session_id FROM ecommerce_lake.events WHERE partition_date = '2026-08-25'
       AND session_id IS NOT NULL) e
  ON g.session_id = e.session_id
HAVING count(*) FILTER (WHERE e.session_id IS NULL) > 0
    OR count(*) FILTER (WHERE g.session_id IS NULL) > 0;


-- ─────────────────────────────────────────────
-- 4. INVARIANTS DE LA COUCHE GOLD
--    (zéro ligne attendue)
-- ─────────────────────────────────────────────

-- 4.1 — L'entonnoir est monotone : on ne peut pas commander sans être passé au
-- paiement, ni payer sans avoir mis au panier.
SELECT partition_date, channel, sessions, viewed, carted, checked_out, ordered
FROM ecommerce_lake.funnel_daily
WHERE partition_date >= cast(current_date - interval '7' day as varchar)
  AND (viewed > sessions OR carted > viewed OR checked_out > carted OR ordered > checked_out);

-- 4.2 — Les taux sont des pourcentages.
SELECT partition_date, channel, view_to_cart_pct, cart_to_checkout_pct,
       checkout_to_order_pct, overall_conversion_pct
FROM ecommerce_lake.funnel_daily
WHERE partition_date >= cast(current_date - interval '7' day as varchar)
  AND (view_to_cart_pct       NOT BETWEEN 0 AND 100
    OR cart_to_checkout_pct   NOT BETWEEN 0 AND 100
    OR checkout_to_order_pct  NOT BETWEEN 0 AND 100
    OR overall_conversion_pct NOT BETWEEN 0 AND 100);

-- 4.3 — `converted` et `bounced` doivent suivre les compteurs de la session.
SELECT session_id, orders, events, views, converted, bounced
FROM ecommerce_lake.sessions
WHERE partition_date = '2026-08-25'
  AND (converted <> (orders > 0) OR bounced <> (events = 1 AND views = 1))
LIMIT 50;

-- 4.4 — Une session ne peut pas finir avant d'avoir commencé.
SELECT session_id, session_start, session_end, duration_seconds
FROM ecommerce_lake.sessions
WHERE partition_date = '2026-08-25'
  AND (session_end < session_start OR duration_seconds < 0)
LIMIT 50;

-- 4.5 — RFM : scores dans 1..5, score composite cohérent, client unique.
SELECT customer_id, r_score, f_score, m_score, rfm_score, rfm_segment
FROM ecommerce_lake.customer_rfm
WHERE r_score NOT BETWEEN 1 AND 5
   OR f_score NOT BETWEEN 1 AND 5
   OR m_score NOT BETWEEN 1 AND 5
   OR rfm_score <> r_score + f_score + m_score
LIMIT 50;

SELECT customer_id, count(*) AS lignes
FROM ecommerce_lake.customer_rfm
GROUP BY customer_id
HAVING count(*) > 1;

-- 4.6 — Un acheteur a des commandes, un non-acheteur n'en a pas.
SELECT customer_id, is_buyer, orders, monetary
FROM ecommerce_lake.customer_rfm
WHERE is_buyer <> (orders > 0)
LIMIT 50;

-- 4.7 — product_daily : le rang par revenu est bien un rang, sans trou ni
-- doublon à l'intérieur d'un jour.
SELECT partition_date, revenue_rank, count(*) AS produits
FROM ecommerce_lake.product_daily
WHERE partition_date = '2026-08-25'
GROUP BY partition_date, revenue_rank
HAVING count(*) > 1;

-- 4.8 — Statut de commande et montant réalisé.
-- Une commande annulée ou remboursée ne peut pas garder tout son revenu.
SELECT order_id, status, cancelled, refunded, net_amount, reversed_amount, realized_revenue
FROM ecommerce_lake.orders
WHERE partition_date = '2026-08-25'
  AND (abs(net_amount - reversed_amount - realized_revenue) > 0.01
       OR (cancelled AND realized_revenue > 0))
LIMIT 50;


-- ─────────────────────────────────────────────
-- 5. FRAÎCHEUR ET COMPLÉTUDE
-- ─────────────────────────────────────────────

-- 5.1 — Quel est le jour le plus récent dans chaque couche ?
-- Un gold en retard sur le silver = silver_to_gold n'a pas tourné.
SELECT 'silver/events' AS dataset, max(partition_date) AS dernier_jour FROM ecommerce_lake.events
UNION ALL SELECT 'gold/sessions',     max(partition_date) FROM ecommerce_lake.sessions
UNION ALL SELECT 'gold/orders',       max(partition_date) FROM ecommerce_lake.orders
UNION ALL SELECT 'gold/funnel_daily', max(partition_date) FROM ecommerce_lake.funnel_daily
UNION ALL SELECT 'gold/product_daily',max(partition_date) FROM ecommerce_lake.product_daily
UNION ALL SELECT 'gold/anomalies',    max(partition_date) FROM ecommerce_lake.anomalies
ORDER BY dataset;

-- 5.2 — Les jours manquants sur les 30 derniers.
WITH calendrier AS (
    SELECT cast(jour as varchar) AS partition_date
    FROM UNNEST(sequence(current_date - interval '30' day, current_date, interval '1' day)) AS t(jour)
),
presents AS (
    SELECT DISTINCT partition_date FROM ecommerce_lake.events
)
SELECT c.partition_date AS jour_manquant
FROM calendrier c
LEFT JOIN presents p ON p.partition_date = c.partition_date
WHERE p.partition_date IS NULL
ORDER BY jour_manquant;


-- ─────────────────────────────────────────────
-- 6. LES ANOMALIES DÉTECTÉES PAR LE PIPELINE
--    Ici on lit, on ne contrôle pas : ces lignes sont le produit du job.
-- ─────────────────────────────────────────────

-- Ce que gold/anomalies a levé, du plus grave au moins grave.
SELECT
    partition_date,
    severity,
    array_join(reasons, ' | ') AS motifs,
    count(*)                   AS occurrences
FROM ecommerce_lake.anomalies
WHERE partition_date >= cast(current_date - interval '7' day as varchar)
GROUP BY partition_date, severity, array_join(reasons, ' | ')
ORDER BY partition_date DESC, occurrences DESC;

-- Le détail d'un motif précis.
SELECT occurred_ts, event_type, session_id, customer_id, order_id,
       quantity, discount_pct, net_amount, reasons, severity
FROM ecommerce_lake.anomalies
WHERE partition_date = '2026-08-25'
  AND contains(reasons, 'discount_out_of_range')
ORDER BY occurred_ts
LIMIT 100;

-- Le taux d'anomalies rapporté au volume du jour — c'est le chiffre à
-- surveiller, pas le compte brut.
SELECT
    a.partition_date,
    a.anomalies,
    e.evenements,
    round(100.0 * a.anomalies / nullif(e.evenements, 0), 3) AS taux_pct
FROM (SELECT partition_date, count(*) AS anomalies FROM ecommerce_lake.anomalies
       WHERE partition_date >= cast(current_date - interval '7' day as varchar)
       GROUP BY partition_date) a
JOIN (SELECT partition_date, count(*) AS evenements FROM ecommerce_lake.events
       WHERE partition_date >= cast(current_date - interval '7' day as varchar)
       GROUP BY partition_date) e
  ON e.partition_date = a.partition_date
ORDER BY a.partition_date DESC;


-- ─────────────────────────────────────────────
-- 7. LE LAC ET L'ENTREPÔT DISENT-ILS LA MÊME CHOSE
--
-- Athena ne lit pas PostgreSQL : lancer la requête ici, la même dans psql, et
-- comparer les deux résultats à la main. C'est le contrôle qui valide
-- glue_rds_load — celui qu'aucun test unitaire ne peut faire.
-- ─────────────────────────────────────────────

-- Côté lac (Athena) :
SELECT partition_date, count(*) AS evenements, round(sum(signed_net_amount), 2) AS revenu
FROM ecommerce_lake.events
WHERE partition_date = '2026-08-25'
GROUP BY partition_date;

-- Côté entrepôt (psql), doit renvoyer exactement la même ligne :
--   SELECT partition_date, count(*) AS evenements, round(sum(signed_net_amount), 2) AS revenu
--   FROM analytics.fact_events
--   WHERE partition_date = '2026-08-25'
--   GROUP BY partition_date;
