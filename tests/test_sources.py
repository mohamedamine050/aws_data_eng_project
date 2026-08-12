"""Unit tests for common.sources — the input connectors."""

import json

import pytest

from common import sources


# ── PARSERS ──────────────────────────────────────────────────

def test_parse_json_payload_array():
    assert sources.parse_json_payload('[{"a": 1}, {"b": 2}]') == [{"a": 1}, {"b": 2}]


def test_parse_json_payload_wrapped():
    assert sources.parse_json_payload('{"products": [{"id": 1}]}') == [{"id": 1}]


def test_parse_json_payload_single_object():
    assert sources.parse_json_payload('{"id": 1}') == [{"id": 1}]


def test_parse_json_payload_empty():
    assert sources.parse_json_payload("   ") == []


def test_parse_ndjson_skips_bad_lines():
    text = '{"id": 1}\nnot-json\n\n{"id": 2}\n'
    assert sources.parse_ndjson(text) == [{"id": 1}, {"id": 2}]


def test_parse_json_payload_falls_back_to_ndjson():
    text = '{"id": 1}\n{"id": 2}'
    assert sources.parse_json_payload(text) == [{"id": 1}, {"id": 2}]


def test_parse_csv_maps_headers_and_blanks():
    text = "product_id,name,price\nsku-1,Mouse,19.99\nsku-2,,5\n"
    rows = sources.parse_csv(text)
    assert rows[0] == {"product_id": "sku-1", "name": "Mouse", "price": "19.99"}
    assert rows[1]["name"] is None


def test_parse_csv_empty():
    assert sources.parse_csv("") == []


def test_parse_auto_uses_extension():
    assert sources.parse_auto("a,b\n1,2\n", "catalog.csv") == [{"a": "1", "b": "2"}]
    assert sources.parse_auto('[{"a": 1}]', "catalog.json") == [{"a": 1}]


def test_parse_auto_sniffs_without_extension():
    assert sources.parse_auto('[{"a": 1}]', "blob") == [{"a": 1}]
    assert sources.parse_auto("a,b\n1,2\n", "blob") == [{"a": "1", "b": "2"}]


# ── FIELD MAPPING ────────────────────────────────────────────

def test_normalize_product_accepts_api_shape():
    product = sources.normalize_product({
        "id": 7, "title": "Frozen Table", "category": {"name": "Home"}, "price": 15.5,
    })
    assert product == {
        "product_id": "7", "sku": "7", "name": "Frozen Table",
        "category": "Home", "brand": None, "price": 15.5,
    }


def test_normalize_product_accepts_csv_strings():
    product = sources.normalize_product({
        "product_id": "sku-9", "name": "Lamp", "price": "12,50", "category": "Home", "brand": "Ikea",
    })
    assert product["price"] == 12.5
    assert product["brand"] == "Ikea"


@pytest.mark.parametrize("entry", [
    {"name": "No id", "price": 1.0},
    {"product_id": "x", "price": 1.0},
    {"product_id": "x", "name": "Bad price", "price": "abc"},
    {"product_id": "x", "name": "Negative", "price": -1},
    "not-a-dict",
])
def test_normalize_product_rejects_unusable_rows(entry):
    assert sources.normalize_product(entry) is None


def test_normalize_customer():
    customer = sources.normalize_customer({"id": 4, "tier": "vip", "country_code": "FR"})
    assert customer == {"customer_id": "4", "segment": "vip", "country": "FR", "city": None}


def test_normalize_customer_requires_id():
    assert sources.normalize_customer({"tier": "vip"}) is None


# ── RESOLUTION ───────────────────────────────────────────────

def test_load_products_merges_inline_and_file(tmp_path):
    catalog = tmp_path / "catalog.csv"
    catalog.write_text("product_id,name,price\nsku-2,Keyboard,49.9\n", encoding="utf-8")

    products, stats = sources.load_products({
        "PRODUCTS": [{"product_id": "sku-1", "name": "Mouse", "price": 19.99}],
        "PRODUCTS_LOCAL": str(catalog),
    })

    assert [p["product_id"] for p in products] == ["sku-1", "sku-2"]
    assert stats["sources_used"] == ["PRODUCTS", "PRODUCTS_LOCAL"]
    assert stats["resolved"] == 2


def test_load_products_deduplicates_across_sources(tmp_path):
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps([{"product_id": "sku-1", "name": "Mouse", "price": 19.99}]), encoding="utf-8")

    products, stats = sources.load_products({
        "PRODUCTS": [{"product_id": "sku-1", "name": "Mouse", "price": 19.99}],
        "PRODUCTS_LOCAL": str(catalog),
    })

    assert len(products) == 1
    assert stats["duplicates_removed"] == 1


def test_load_products_counts_dropped_rows():
    products, stats = sources.load_products({
        "PRODUCTS": [
            {"product_id": "sku-1", "name": "Good", "price": 10.0},
            {"name": "No id", "price": 5.0},
        ],
    })
    assert len(products) == 1
    assert stats["rows_dropped"] == 1


def test_load_products_survives_a_failing_source(tmp_path, caplog):
    """A missing catalog file must degrade the run, not abort it."""
    products, stats = sources.load_products({
        "PRODUCTS": [{"product_id": "sku-1", "name": "Good", "price": 10.0}],
        "PRODUCTS_LOCAL": str(tmp_path / "does-not-exist.json"),
    })

    assert len(products) == 1
    assert stats["sources_used"] == ["PRODUCTS"]


def test_load_products_survives_a_failing_api(monkeypatch):
    def boom(url, timeout=10):
        raise TimeoutError("catalog API down")

    monkeypatch.setattr(sources, "read_http_json", boom)

    products, stats = sources.load_products({
        "PRODUCTS": [{"product_id": "sku-1", "name": "Good", "price": 10.0}],
        "ECOMMERCE_API_URL": "https://example.test/products",
    })

    assert len(products) == 1
    assert "ECOMMERCE_API_URL" not in stats["sources_used"]


def test_load_customers_from_local_csv(tmp_path):
    path = tmp_path / "customers.csv"
    path.write_text("customer_id,segment,country\ncust-1,vip,FR\n", encoding="utf-8")

    customers, stats = sources.load_customers({"CUSTOMERS_LOCAL": str(path)})

    assert customers == [{"customer_id": "cust-1", "segment": "vip", "country": "FR", "city": None}]
    assert stats["resolved"] == 1


def test_read_text_dispatches_to_s3(monkeypatch):
    class DummyS3:
        def get_object(self, Bucket, Key):
            assert (Bucket, Key) == ("demo", "catalog/products.json")
            return {"Body": type("B", (), {"read": lambda self: b'{"ok": true}'})()}

    monkeypatch.setattr(sources, "_s3_client", lambda: DummyS3())
    assert sources.read_text("s3://demo/catalog/products.json") == '{"ok": true}'
