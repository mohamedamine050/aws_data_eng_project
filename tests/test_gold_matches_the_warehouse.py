"""Les colonnes que gold produit doivent exister dans les tables PostgreSQL.

Le job 4 ecrit chaque table gold en JDBC. Spark envoie les colonnes du
DataFrame telles quelles : une colonne que la table n'a pas fait echouer
l'ecriture, et un run de 40 minutes meurt a la derniere etape. Rien ne
comparait les deux cotes.
"""

import pathlib
import re
from datetime import datetime, timezone

import pytest

import jobs.glue_silver_to_gold as gold
import jobs.glue_bronze_to_silver as silver
from common.event_simulator import simulate

SCHEMA_SQL = pathlib.Path(__file__).resolve().parent.parent / "sql/warehouse/001_schema.sql"

#: dataset gold -> table de l'entrepot
GOLD_TO_TABLE = {
    "sessions": "analytics.fact_sessions",
    "funnel_daily": "analytics.agg_funnel_daily",
    "orders": "analytics.fact_orders",
    "customer_rfm": "analytics.dim_customer_rfm",
    "product_daily": "analytics.agg_product_daily",
    "anomalies": "analytics.fact_anomalies",
}

_STRUCTURAL = {"primary", "unique", "foreign", "constraint", "check", "exclude"}


def _table_columns() -> dict:
    """Les colonnes declarees par ``001_schema.sql``, par table."""
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    tables = {}
    for match in re.finditer(
        r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+([\w.\"]+)\s*\((.*?)\n\);", sql, re.S | re.I
    ):
        name = match.group(1).replace('"', "").lower()
        columns = []
        for line in match.group(2).splitlines():
            line = line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            first = line.split()[0].strip('"').lower()
            if first not in _STRUCTURAL:
                columns.append(first)
        tables[name] = columns
    return tables


@pytest.fixture(scope="module")
def built(spark):
    """Les six tables gold, construites une fois sur du trafic simule."""
    products = [
        {"product_id": f"sku-{i}", "sku": f"SKU-{i}", "name": f"Product {i}",
         "category": "electronics", "price": 20.0 * i}
        for i in range(1, 6)
    ]
    records = simulate(
        products,
        {"SEED": 11, "SESSIONS": 40, "CUSTOMER_POOL": 10, "WINDOW_MINUTES": 120},
        now=datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc),
    )
    rows = spark.sparkContext.parallelize([__import__("json").dumps(r) for r in records])
    raw = spark.read.json(rows)
    processed = silver.to_processed(raw).cache()
    processed.count()

    return {
        "sessions": gold.build_sessions(processed),
        "funnel_daily": gold.build_funnel_daily(processed),
        "orders": gold.build_orders(processed),
        "customer_rfm": gold.build_customer_rfm(processed),
        "product_daily": gold.build_product_daily(processed, top_n=0),
        "anomalies": gold.build_anomalies(processed),
    }


@pytest.mark.parametrize("dataset", sorted(GOLD_TO_TABLE))
def test_every_gold_column_exists_in_its_warehouse_table(dataset, built):
    """Une colonne que la table n'a pas fait echouer l'ecriture JDBC."""
    table = GOLD_TO_TABLE[dataset]
    declared = _table_columns()[table]
    produced = [column.lower() for column in built[dataset].columns]

    unknown = [column for column in produced if column not in declared]

    assert not unknown, (
        f"{dataset} produit des colonnes absentes de {table}: {unknown}\n"
        f"  produites : {produced}\n"
        f"  declarees : {declared}"
    )


@pytest.mark.parametrize("dataset", sorted(GOLD_TO_TABLE))
def test_no_warehouse_column_is_left_unfilled(dataset, built):
    """L'inverse : une colonne de la table que gold ne remplit jamais.

    Ce n'est pas une erreur d'ecriture — Spark laisse la colonne a NULL — mais
    c'est une colonne morte dans l'entrepot, donc une requete BI qui ne renverra
    rien. Les colonnes a defaut SQL sont exclues.
    """
    table = GOLD_TO_TABLE[dataset]
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    body = re.search(
        rf"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+{re.escape(table)}\s*\((.*?)\n\);",
        sql, re.S | re.I,
    ).group(1)

    with_default = set()
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        first = line.split()[0].strip('"').lower()
        if first in _STRUCTURAL:
            continue
        if "default" in line.lower() or "generated" in line.lower() or "serial" in line.lower():
            with_default.add(first)

    declared = [c for c in _table_columns()[table] if c not in with_default]
    produced = {column.lower() for column in built[dataset].columns}

    unfilled = [column for column in declared if column not in produced]

    assert not unfilled, f"{table} declare des colonnes que {dataset} ne remplit jamais: {unfilled}"
