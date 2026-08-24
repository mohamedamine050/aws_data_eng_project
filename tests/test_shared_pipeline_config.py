"""Un seul fichier de config doit satisfaire les quatre jobs.

C'est ainsi qu'il est deploye : le meme --CONFIG_PATH pour les quatre. Rien ne
verifiait qu'un fichier partage ne fait pas se marcher dessus deux jobs, ni que
les chemins qu'il produit se raccordent bout a bout.
"""

import json
import pathlib

import pytest

import jobs.glue_bronze_to_silver as job2
import jobs.glue_landing_ingest as job1
import jobs.glue_rds_load as job4
import jobs.glue_silver_to_gold as job3

CONFIG_FILE = pathlib.Path(__file__).resolve().parent.parent / "config/pipeline.example.json"

JOBS = {"landing_ingest": job1, "bronze_to_silver": job2, "silver_to_gold": job3, "rds_load": job4}


def _config() -> dict:
    return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))


def test_the_file_is_valid_json_and_names_a_bucket():
    config = _config()

    assert config["OUTPUT_BUCKET"], "OUTPUT_BUCKET est le seul reglage obligatoire"
    assert not config["OUTPUT_BUCKET"].startswith("s3://"), "un nom de bucket, pas une URI"


def test_it_does_not_pin_job_name():
    """Glue passe --JOB_NAME ; le figer ici ferait signer les 4 jobs pareil."""
    assert "JOB_NAME" not in _config()


@pytest.mark.parametrize("name", sorted(JOBS))
def test_every_job_resolves_its_paths_from_this_one_file(name):
    paths = JOBS[name].build_paths(_config())

    for key in ("bronze_events", "silver_events", "quarantine_events"):
        assert paths[key].startswith("s3://"), f"{name}: {key} mal resolu"


def test_the_stages_join_up_end_to_end():
    """Ce qu'un etage ecrit est ce que le suivant lit — avec CE fichier."""
    config = _config()

    assert job1.build_paths(config)["bronze_events"] == job2.build_paths(config)["bronze_events"]
    assert job2.build_paths(config)["silver_events"] == job3.build_paths(config)["silver_events"]

    targets = {target["dataset"]: target["path"] for target in job4.resolve_targets(config)}
    assert targets["silver/events"] == job2.build_paths(config)["silver_events"]
    for dataset in job3.plan(config):
        assert targets[f"gold/{dataset}"] == job3.gold_path(config, dataset)


def test_the_drop_zone_sits_under_landing():
    config = _config()

    assert job1.ingest_prefix(config) == "landing/partners/"
    assert job1.archive_prefix(config) == "landing/_processed/"


def test_gold_builds_exactly_what_the_warehouse_load_expects():
    config = _config()

    built = set(job3.plan(config))
    loaded = {
        target["dataset"].split("/", 1)[1]
        for target in job4.resolve_targets(config)
        if target["dataset"].startswith("gold/")
    }

    assert built == loaded


def test_the_warehouse_connection_only_lacks_the_secrets():
    """Host et password sont a remplir ; le reste doit deja etre bon."""
    with pytest.raises(ValueError) as excinfo:
        job4._resolve_rds_settings(_config())

    missing = str(excinfo.value)
    assert "RDS_HOST" in missing and "RDS_PASSWORD" in missing
    assert "RDS_DATABASE" not in missing
    assert "RDS_PORT" not in missing


def test_it_connects_once_host_and_password_are_filled():
    config = {**_config(), "RDS_HOST": "warehouse.rds.amazonaws.com", "RDS_PASSWORD": "s3cret"}

    settings = job4._resolve_rds_settings(config)

    assert job4._build_jdbc_url(settings) == (
        "jdbc:postgresql://warehouse.rds.amazonaws.com:5432/ecommerce?sslmode=require"
    )


def test_no_job_reads_a_key_another_job_defines_differently():
    """Le risque du fichier partage : deux jobs, deux sens pour une meme cle."""
    per_job = {}
    for path in sorted((CONFIG_FILE.parent).glob("glue_*.example.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for key, value in payload.items():
            if key.startswith("_") or key == "JOB_NAME":
                continue
            per_job.setdefault(key, {})[path.stem] = value

    conflicts = {
        key: values for key, values in per_job.items()
        if len({json.dumps(v, sort_keys=True) for v in values.values()}) > 1
    }

    assert not conflicts, f"cles a sens divergent selon le job: {sorted(conflicts)}"
