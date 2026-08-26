"""La creation du schema PostgreSQL avant l'ecriture JDBC.

Spark ne cree pas le schema : ``.save()`` sur ``analytics.fact_events`` echoue
avec ``schema "analytics" does not exist`` tant que personne ne l'a cree. Le job
ouvre donc une connexion JDBC directe, via le gateway JVM de Spark, et emet un
``CREATE SCHEMA IF NOT EXISTS`` avant la premiere ecriture.

Ce chemin ne passe par aucune API Spark testable en local : il descend dans
``sparkContext._gateway.jvm.java.sql.DriverManager``. Les doubles ci-dessous
rejouent cette chaine d'attributs et enregistrent ce qui a ete execute.
"""

import json

import pytest

import jobs.glue_rds_load as rds


# ─────────────────────────────────────────────
# LES DOUBLES JDBC
# ─────────────────────────────────────────────

class _Statement:
    def __init__(self, fail=None):
        self._fail = fail
        self.executed = []
        self.closed = False

    def executeUpdate(self, sql):
        self.executed.append(sql)
        if self._fail is not None:
            raise self._fail
        return 0

    def close(self):
        self.closed = True


class _Connection:
    def __init__(self, fail=None):
        self.statement = _Statement(fail)
        self.autocommit = None
        self.closed = False

    def setAutoCommit(self, flag):
        self.autocommit = flag

    def createStatement(self):
        return self.statement

    def close(self):
        self.closed = True


class _DriverManager:
    def __init__(self, fail=None):
        self._fail = fail
        self.connection = None
        self.calls = []

    def getConnection(self, url, user, password):
        self.calls.append((url, user, password))
        self.connection = _Connection(self._fail)
        return self.connection


class _Spark:
    """Juste assez de Spark pour atteindre ``jvm.java.sql.DriverManager``."""

    def __init__(self, driver_manager):
        sql = type("_sql", (), {"DriverManager": driver_manager})
        java = type("_java", (), {"sql": sql})
        jvm = type("_jvm", (), {"java": java})
        gateway = type("_gateway", (), {"jvm": jvm})
        self.sparkContext = type("_sc", (), {"_gateway": gateway})


def _settings(**overrides):
    return {
        "host": "db.example",
        "port": "5432",
        "database": "ecommerce",
        "username": "adminuser",
        "password": "s3cret",
        "schema": "analytics",
        "sslmode": "require",
        **overrides,
    }


# ─────────────────────────────────────────────
# CE QUI EST REELLEMENT EXECUTE
# ─────────────────────────────────────────────

def test_the_schema_is_created_if_it_does_not_exist():
    """``IF NOT EXISTS`` : le job tourne tous les jours, pas une seule fois."""
    driver = _DriverManager()

    rds._ensure_schema(_Spark(driver), _settings())

    assert driver.connection.statement.executed == [
        'CREATE SCHEMA IF NOT EXISTS "analytics"'
    ]


def test_the_schema_name_is_quoted():
    """Sans guillemets, PostgreSQL replie l'identifiant en minuscules et le
    schema cree ne serait pas celui que ``_qualified`` ecrit ensuite."""
    driver = _DriverManager()

    rds._ensure_schema(_Spark(driver), _settings(schema="Analytics"))

    assert '"Analytics"' in driver.connection.statement.executed[0]


def test_the_connection_uses_the_job_credentials_and_url():
    driver = _DriverManager()
    settings = _settings()

    rds._ensure_schema(_Spark(driver), settings)

    assert driver.calls == [
        (rds._build_jdbc_url(settings), "adminuser", "s3cret"),
    ]


def test_the_create_is_committed():
    """Sans autocommit le CREATE part avec la connexion fermee : le schema
    n'existe pas, et l'ecriture JDBC qui suit echoue quand meme."""
    driver = _DriverManager()

    rds._ensure_schema(_Spark(driver), _settings())

    assert driver.connection.autocommit is True


# ─────────────────────────────────────────────
# LA CONNEXION EST TOUJOURS RENDUE
#
# RDS plafonne les connexions. Une connexion fuitee par run finit par refuser
# le job lui-meme, et le chemin d'echec est celui qui fuit le plus souvent.
# ─────────────────────────────────────────────

def test_the_connection_is_closed_after_a_successful_create():
    driver = _DriverManager()

    rds._ensure_schema(_Spark(driver), _settings())

    assert driver.connection.statement.closed is True
    assert driver.connection.closed is True


def test_a_failed_create_still_closes_the_connection():
    driver = _DriverManager(fail=RuntimeError("permission denied for database"))

    with pytest.raises(RuntimeError, match="permission denied"):
        rds._ensure_schema(_Spark(driver), _settings())

    assert driver.connection.statement.closed is True
    assert driver.connection.closed is True


def test_a_failed_create_stops_the_job():
    """Continuer signifie ecrire dans un schema absent : autant echouer ici,
    ou le message dit pourquoi."""
    driver = _DriverManager(fail=RuntimeError("boom"))

    with pytest.raises(RuntimeError):
        rds._ensure_schema(_Spark(driver), _settings())


# ─────────────────────────────────────────────
# LE CAS OU IL N'Y A RIEN A CREER
# ─────────────────────────────────────────────

@pytest.mark.parametrize("schema", ["", None])
def test_no_schema_configured_touches_no_connection(schema):
    """Sans schema, ``_qualified`` ecrit dans le search_path : rien a creer."""
    def refuse(*args, **kwargs):
        pytest.fail("une connexion JDBC a ete ouverte sans schema a creer")

    driver = type("_D", (), {"getConnection": staticmethod(refuse)})()

    rds._ensure_schema(_Spark(driver), _settings(schema=schema))


# ─────────────────────────────────────────────
# L'IDENTIFIANT N'EST PAS PARAMETRABLE
#
# ``CREATE SCHEMA`` n'accepte pas de placeholder : le nom est concatene dans le
# SQL. Il vient du fichier de config, donc il est valide avant, pas apres.
# ─────────────────────────────────────────────

@pytest.mark.parametrize(
    "schema",
    [
        'analytics"; DROP SCHEMA public CASCADE; --',
        "analytics; SELECT 1",
        "public.evil",
        "deux mots",
        "analytics-1",
        "",
    ],
)
def test_an_identifier_that_could_carry_sql_is_refused(schema):
    with pytest.raises(ValueError):
        rds._validate_identifier(schema)


@pytest.mark.parametrize("schema", ["analytics", "Analytics", "_staging", "gold_v2", "s3"])
def test_a_plain_identifier_is_accepted(schema):
    assert rds._validate_identifier(schema) is None


def test_an_unsafe_schema_never_reaches_the_database():
    """La validation passe avant l'ouverture de la connexion."""
    def refuse(*args, **kwargs):
        pytest.fail("le SQL a atteint la base avant d'etre valide")

    driver = type("_D", (), {"getConnection": staticmethod(refuse)})()

    with pytest.raises(ValueError, match="Invalid PostgreSQL identifier"):
        rds._ensure_schema(_Spark(driver), _settings(schema='a"; DROP SCHEMA public; --'))


# ─────────────────────────────────────────────
# LA PLACE DU CREATE DANS LE RUN
#
# C'est tout l'interet du correctif : cree apres la premiere ecriture, le
# schema arrive trop tard.
# ─────────────────────────────────────────────

def test_main_creates_the_schema_before_loading_anything(tmp_path, monkeypatch, capsys):
    order = []

    config = tmp_path / "job.json"
    config.write_text(json.dumps({"OUTPUT_BUCKET": "demo-lake"}), encoding="utf-8")

    class _Builder:
        def appName(self, name):
            return self

        def getOrCreate(self):
            return "spark-session"

    monkeypatch.setattr(rds, "getResolvedOptions", lambda argv, keys: {
        "JOB_NAME": "ecommerce-rds-load", "CONFIG_PATH": str(config),
    })
    monkeypatch.setattr(rds.sys, "argv",
                        ["prog", "--JOB_NAME", "x", "--CONFIG_PATH", str(config)])
    monkeypatch.setattr(rds, "SparkSession", type("S", (), {"builder": _Builder()}))
    monkeypatch.setattr(rds, "_resolve_rds_settings", lambda config: _settings())
    monkeypatch.setattr(rds, "resolve_targets", lambda config: [
        {"dataset": "silver/events", "path": "s3://demo-lake/silver/events/",
         "table": "fact_events", "mode": "append"},
    ])
    monkeypatch.setattr(rds, "_ensure_schema",
                        lambda spark, settings: order.append("ensure_schema"))
    monkeypatch.setattr(rds, "load_targets", lambda spark, targets, settings: (
        order.append("load_targets")
        or [{"table": "analytics.fact_events", "status": "loaded", "rows_loaded": 42}]
    ))

    rds.main()

    assert order == ["ensure_schema", "load_targets"]

    summary = json.loads(capsys.readouterr().out)
    assert summary["status"] == "success"
    assert summary["schema"] == "analytics"
    assert summary["rows_loaded"] == 42


# ---------------------------------------------
# LE SCHEMA PAR DEFAUT
#
# sql/warehouse/001_schema.sql cree analytics. Une config qui ne nomme aucun
# schema visait donc deja analytics sans le dire : c'est maintenant le defaut,
# et le job le cree lui-meme.
# ---------------------------------------------

def _connection(**overrides):
    return {
        "RDS_HOST": "db.example", "RDS_DATABASE": "ecommerce",
        "RDS_USERNAME": "u", "RDS_PASSWORD": "p", **overrides,
    }


def test_a_config_that_names_no_schema_takes_analytics():
    assert rds._resolve_rds_settings(_connection())["schema"] == rds.DEFAULT_SCHEMA
    assert rds.DEFAULT_SCHEMA == "analytics"


def test_a_named_schema_wins_over_the_default():
    settings = rds._resolve_rds_settings(_connection(RDS_SCHEMA="staging"))

    assert settings["schema"] == "staging"
    assert rds._qualified(settings, "fact_events") == "staging.fact_events"


def test_the_default_schema_qualifies_the_tables():
    settings = rds._resolve_rds_settings(_connection())

    assert rds._qualified(settings, "fact_events") == "analytics.fact_events"
