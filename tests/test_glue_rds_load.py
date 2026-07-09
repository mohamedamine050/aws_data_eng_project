import sys
import json
from io import BytesIO
from pathlib import Path

import pytest

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


def test_load_text_local(tmp_path):
    file_path = tmp_path / "config.json"
    file_path.write_text('{"key": "value"}', encoding="utf-8")

    assert glue_job._load_text(str(file_path)) == '{"key": "value"}'


def test_load_text_s3(monkeypatch):
    class DummyS3:
        def get_object(self, Bucket, Key):
            return {"Body": BytesIO(b'{"loaded": true}')}

    monkeypatch.setattr(glue_job.boto3, "client", lambda service: DummyS3())

    assert glue_job._load_text("s3://demo-bucket/config.json") == '{"loaded": true}'


def test_load_config_local(tmp_path):
    file_path = tmp_path / "config.json"
    file_path.write_text('{"OUTPUT_BUCKET": "b"}', encoding="utf-8")

    config = glue_job.load_config(str(file_path))

    assert config["OUTPUT_BUCKET"] == "b"


def test_load_config_s3(monkeypatch):
    class DummyS3:
        def get_object(self, Bucket, Key):
            return {"Body": BytesIO(b'{"OUTPUT_BUCKET": "b"}')}

    monkeypatch.setattr(glue_job.boto3, "client", lambda service: DummyS3())

    assert glue_job.load_config("s3://bucket/config.json")["OUTPUT_BUCKET"] == "b"


def test_build_processed_path_defaults():
    config = {"OUTPUT_BUCKET": "my-bucket"}

    assert glue_job._build_processed_path(config) == "s3://my-bucket/processed/"


def test_build_processed_path_explicit():
    config = {"PROCESSED_S3_PATH": "s3://bucket/data/"}

    assert glue_job._build_processed_path(config) == "s3://bucket/data/"


def test_resolve_rds_settings_missing_raises():
    with pytest.raises(ValueError, match="Missing RDS settings"):
        glue_job._resolve_rds_settings({"RDS_TABLE": "events"})


def test_parse_args_local(monkeypatch):
    monkeypatch.setattr(glue_job, "getResolvedOptions", None)
    monkeypatch.setattr(glue_job.os.sys, "argv", ["prog", "--config", "config.json"])

    result = glue_job._parse_args()

    assert result["config"] == "config.json"
    assert result["mode"] == "local"


def test_parse_args_glue(monkeypatch):
    def fake_resolver(argv, keys):
        return {"JOB_NAME": "job", "CONFIG_PATH": "s3://bucket/config.json"}

    monkeypatch.setattr(glue_job, "getResolvedOptions", fake_resolver)
    monkeypatch.setattr(glue_job.os.sys, "argv", ["prog", "JOB_NAME"])

    result = glue_job._parse_args()

    assert result["config"] == "s3://bucket/config.json"
    assert result["mode"] == "glue"


def test_read_processed_dataset_missing_columns():
    class DummyDataFrame:
        columns = ["event_type"]

        def select(self, *args):
            return self

    class DummySpark:
        class Read:
            def parquet(self, path):
                return DummyDataFrame()

        @property
        def read(self):
            return DummySpark.Read()

    with pytest.raises(ValueError, match="missing columns"):
        glue_job._read_processed_dataset(DummySpark(), "s3://bucket/processed/")


def test_read_processed_dataset_selects_columns():
    class DummyDataFrame:
        columns = glue_job.REQUIRED_COLUMNS

        def __init__(self):
            self.selected = None

        def select(self, *args):
            self.selected = args
            return self

    class DummySpark:
        class Read:
            def parquet(self, path):
                return DummyDataFrame()

        @property
        def read(self):
            return DummySpark.Read()

    result = glue_job._read_processed_dataset(DummySpark(), "s3://bucket/processed/")

    assert result.selected == tuple(glue_job.REQUIRED_COLUMNS)


def test_write_to_rds(monkeypatch):
    class DummyWriter:
        def __init__(self):
            self.calls = []

        def format(self, value):
            self.calls.append(("format", value))
            return self

        def option(self, key, value):
            self.calls.append((key, value))
            return self

        def mode(self, value):
            self.calls.append(("mode", value))
            return self

        def save(self):
            self.calls.append(("save", None))

    class DummyDataFrame:
        def __init__(self):
            self.write = DummyWriter()

        def count(self):
            return 2

    monkeypatch.setattr(glue_job, "_build_jdbc_url", lambda settings: "jdbc:postgresql://host:5432/db")

    dataframe = DummyDataFrame()
    settings = {
        "username": "user",
        "password": "pass",
        "driver": "org.postgresql.Driver",
        "write_mode": "append",
        "table": "events",
    }

    glue_job._write_to_rds(dataframe, settings)

    assert ("format", "jdbc") in dataframe.write.calls
    assert ("dbtable", "events") in dataframe.write.calls
    assert ("user", "user") in dataframe.write.calls
    assert ("password", "pass") in dataframe.write.calls
    assert ("mode", "append") in dataframe.write.calls
    assert ("save", None) in dataframe.write.calls


def test_load_secret(monkeypatch):
    class DummySecrets:
        def get_secret_value(self, SecretId):
            return {"SecretString": json.dumps({"username": "user", "password": "pass"})}

    monkeypatch.setattr(glue_job.boto3, "client", lambda service: DummySecrets())

    result = glue_job._load_secret("arn:aws:secretsmanager:region:123456789012:secret:test")

    assert result["username"] == "user"
    assert result["password"] == "pass"
