import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(_SRC))

import jobs.glue_rds_load as glue_job


def test_resolve_rds_settings_from_secret(monkeypatch):
    secret = {
        "host": "db.example",
        "port": 5432,
        "dbname": "analytics",
        "username": "dbuser",
        "password": "s3cr3t",
    }

    monkeypatch.setattr(glue_job, "_load_secret", lambda arn: secret)

    config = {"RDS_SECRET_ARN": "arn:aws:secretsmanager:...", "RDS_TABLE": "events"}

    settings = glue_job._resolve_rds_settings(config)

    assert settings["host"] == "db.example"
    assert settings["port"] == "5432"
    assert settings["database"] == "analytics"
    assert settings["username"] == "dbuser"
    assert settings["password"] == "s3cr3t"
    assert settings["table"] == "events"


def test_resolve_rds_settings_from_config():
    config = {
        "RDS_HOST": "db2.example",
        "RDS_PORT": 5432,
        "RDS_DATABASE": "db2",
        "RDS_USERNAME": "u",
        "RDS_PASSWORD": "p",
        "RDS_TABLE": "t",
    }

    settings = glue_job._resolve_rds_settings(config)

    assert settings["host"] == "db2.example"
    assert settings["port"] == "5432"
    assert settings["database"] == "db2"
    assert settings["username"] == "u"
    assert settings["password"] == "p"
    assert settings["table"] == "t"


def test_build_jdbc_url():
    settings = {"host": "db", "port": "5432", "database": "mydb", "sslmode": "require"}
    url = glue_job._build_jdbc_url(settings)

    assert url.startswith("jdbc:postgresql://db:5432/mydb")
    assert "sslmode=require" in url
