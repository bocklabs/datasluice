"""SQL injection regression tests for the DuckDB integration (SEC-03/QUAL-07)."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("duckdb")

import duckdb  # noqa: E402

from datasluice.integrations.duckdb import _validate_table_name, resource_to_relation  # noqa: E402

RESOURCE_URL_INJECTIONS = [
    "data'); DROP TABLE x;--.csv",
    "data' UNION SELECT * FROM secrets--.csv",
    "data; INSERT INTO t VALUES(1).csv",
    "evil'); DROP TABLE y;--.parquet",
    "evil' OR '1'='1.json",
]


@pytest.mark.parametrize("payload", RESOURCE_URL_INJECTIONS)
def test_resource_url_injection_is_a_file_not_found_error(payload: str) -> None:
    con = duckdb.connect()
    with pytest.raises(duckdb.IOException):
        resource_to_relation(payload, con)


@pytest.mark.parametrize("payload", RESOURCE_URL_INJECTIONS)
def test_resource_url_injection_has_no_sql_side_effect(payload: str) -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE victim(id INTEGER)")
    con.execute("INSERT INTO victim VALUES (1), (2), (3)")
    with pytest.raises(duckdb.IOException):
        resource_to_relation(payload, con)
    assert con.execute("SELECT count(*) FROM victim").fetchall() == [(3,)]


BAD_TABLE_NAMES = [
    "x; DROP TABLE v",
    "bad name",
    "1lead",
    'a"; SELECT',
    "dash-name",
    "",
]


@pytest.mark.parametrize("bad_name", BAD_TABLE_NAMES)
def test_table_name_injection_rejected(bad_name: str) -> None:
    with pytest.raises(ValueError):
        _validate_table_name(bad_name)


@pytest.mark.parametrize("bad_name", BAD_TABLE_NAMES)
def test_table_name_injection_rejected_in_relation(bad_name: str) -> None:
    con = duckdb.connect()
    with pytest.raises(ValueError):
        resource_to_relation("data.csv", con, table_name=bad_name)


@pytest.mark.parametrize("good_name", ["resource", "my_table", "t1", "_under"])
def test_table_name_valid(good_name: str) -> None:
    assert _validate_table_name(good_name) == good_name


def test_relation_api_reads_real_csv(tmp_path: Path) -> None:
    csv = tmp_path / "people.csv"
    csv.write_text("id,name\n1,ada\n2,grace\n")
    con = duckdb.connect()
    resource_to_relation(str(csv), con, table_name="people")
    rows = con.execute("SELECT name FROM people ORDER BY id").fetchall()
    assert rows == [("ada",), ("grace",)]


def test_query_resource_is_opt_in_passthrough(tmp_path: Path) -> None:
    csv = tmp_path / "n.csv"
    csv.write_text("v\n5\n")
    con = duckdb.connect()
    from datasluice.integrations.duckdb import query_resource

    rows = query_resource(str(csv), "SELECT sum(v) AS s FROM resource", con)
    assert rows == [(5,)]
