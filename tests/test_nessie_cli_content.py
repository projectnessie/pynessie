# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for `content CLI command`."""
from typing import List, Optional

import pytest
import simplejson
from assertpy import assert_that

from pynessie.model import (
    ContentSchema,
    DeltaLakeTable,
    EntrySchema,
    IcebergTable,
    IcebergView,
    ReferenceSchema,
)

from .conftest import execute_cli_command, make_commit, ref_hash

CONTENT_COMMAND = "content"


@pytest.mark.nessieserver
def test_content_view() -> None:
    """Test content view."""
    branch = "contents_view_dev"
    execute_cli_command(["branch", branch])

    iceberg_table = _create_iceberg_table()
    delta_lake_table = _create_delta_lake_table()
    iceberg_view = _create_iceberg_view()

    make_commit("this_is_iceberg_foo", iceberg_table, branch)
    make_commit("this_is_delta_bar", delta_lake_table, branch)
    make_commit('this_is_"sql.baz"', iceberg_view, branch)

    result_table = ContentSchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "view", "--ref", branch, "this_is_iceberg_foo"]), many=True
    )

    assert_that(result_table).is_length(1)
    iceberg_table.id = result_table[0].id
    assert_that(result_table[0]).is_equal_to(iceberg_table)

    branch_hash = ref_hash(branch)
    result_table_2 = ContentSchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "view", "--ref", branch_hash, "this_is_iceberg_foo"]), many=True
    )
    assert_that(result_table_2).is_equal_to(result_table)
    result_table_2 = ContentSchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "view", "--ref", f"{branch}@{branch_hash}", "this_is_iceberg_foo"]), many=True
    )
    assert_that(result_table_2).is_equal_to(result_table)

    result_table = ContentSchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "view", "--ref", branch, "this_is_delta_bar"]), many=True
    )

    assert_that(result_table).is_length(1)
    delta_lake_table.id = result_table[0].id
    assert_that(result_table[0]).is_equal_to(delta_lake_table)

    result_table = ContentSchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "view", "--ref", branch, 'this_is_"sql.baz"']), many=True
    )

    assert_that(result_table).is_length(1)
    iceberg_view.id = result_table[0].id
    assert_that(result_table[0]).is_equal_to(iceberg_view)


@pytest.mark.nessieserver
def test_content_list() -> None:
    """Test content list."""
    branch = "contents_list_dev"
    execute_cli_command(["branch", branch])

    iceberg_table = _create_iceberg_table()
    delta_lake_table = _create_delta_lake_table()
    iceberg_view = _create_iceberg_view()

    make_commit("this_is_iceberg_foo", iceberg_table, branch)
    make_commit("this_is_delta_bar", delta_lake_table, branch)
    make_commit("this_is_sql_baz", iceberg_view, branch)

    tables = EntrySchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--type", "ICEBERG_TABLE"]), many=True
    )
    assert_that(tables).is_length(1)
    assert_that(tables[0].kind).is_equal_to("ICEBERG_TABLE")

    branch_hash = ref_hash(branch)
    tables_2 = EntrySchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch_hash, "--type", "ICEBERG_TABLE"]), many=True
    )
    assert_that(tables_2).is_equal_to(tables)
    tables_2 = EntrySchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", f"{branch}@{branch_hash}", "--type", "ICEBERG_TABLE"]), many=True
    )
    assert_that(tables_2).is_equal_to(tables)

    tables = EntrySchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--type", "DELTA_LAKE_TABLE"]), many=True
    )
    assert_that(tables).is_length(1)
    assert_that(tables[0].kind).is_equal_to("DELTA_LAKE_TABLE")

    result = execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--filter", "entry.contentType == 'ICEBERG_TABLE'"])
    tables = EntrySchema().loads(result, many=True)
    assert_that(tables).is_length(1)
    assert_that(tables[0].kind).is_equal_to("ICEBERG_TABLE")

    result = execute_cli_command(
        ["--json", CONTENT_COMMAND, "list", "--ref", branch, "--filter", "entry.contentType in ['ICEBERG_TABLE', 'DELTA_LAKE_TABLE']"]
    )
    tables = EntrySchema().loads(result, many=True)
    assert_that(tables).is_length(2)
    assert_that(set(t.kind for t in tables)).is_equal_to({"DELTA_LAKE_TABLE", "ICEBERG_TABLE"})

    result = execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--filter", "entry.name.startsWith('this_is_del')"])
    tables = EntrySchema().loads(result, many=True)
    assert_that(tables).is_length(1)
    assert_that(tables[0].kind).is_equal_to("DELTA_LAKE_TABLE")

    result = execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--filter", "entry.name.startsWith('this_is')"])
    tables = EntrySchema().loads(result, many=True)
    assert_that(tables).is_length(3)
    assert_that(set(i.kind for i in tables)).is_equal_to({"ICEBERG_TABLE", "ICEBERG_VIEW", "DELTA_LAKE_TABLE"})


@pytest.mark.nessieserver
def test_content_commit_delete() -> None:
    """Test content commit delete operation."""
    branch = "contents_commit_delete_dev"
    execute_cli_command(["branch", branch])

    iceberg_table = _create_iceberg_table()

    make_commit("this_is_iceberg_foo", iceberg_table, branch)

    tables = EntrySchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--type", "ICEBERG_TABLE"]), many=True
    )
    assert_that(tables).is_length(1)

    head_hash = _get_head_branch_hash(branch)

    execute_cli_command(
        [CONTENT_COMMAND, "commit", "-R", "--ref", branch, "-m", "delete test table", "-c", head_hash, "this_is_iceberg_foo"]
    )

    tables = EntrySchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--type", "ICEBERG_TABLE"]), many=True
    )
    assert_that(tables).is_length(0)


@pytest.mark.nessieserver
def test_content_commit_with_empty_content_delete() -> None:
    """Test content commit delete with empty content operation."""
    branch = "contents_commit_with_empty_content_delete_dev"
    execute_cli_command(["branch", branch])

    iceberg_table = _create_iceberg_table()

    make_commit("this_is_iceberg_foo", iceberg_table, branch)

    tables = EntrySchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--type", "ICEBERG_TABLE"]), many=True
    )
    assert_that(tables).is_length(1)

    head_hash = _get_head_branch_hash(branch)

    execute_cli_command(
        [CONTENT_COMMAND, "commit", "--stdin", "this_is_iceberg_foo", "--ref", branch, "-m", "delete table", "-c", head_hash],
        input_data="",
    )

    tables = EntrySchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--type", "ICEBERG_TABLE"]), many=True
    )
    assert_that(tables).is_length(0)


@pytest.mark.nessieserver
def test_content_commit_with_edited_data() -> None:
    """Test content commit to edit data operation."""
    branch = "content_commit_with_edited_data_dev"
    execute_cli_command(["branch", branch])

    iceberg_table = _create_iceberg_table()
    table_id = "this_is_iceberg_foo"

    make_commit(table_id, iceberg_table, branch)

    edited_iceberg_table = _create_iceberg_table(metadata_location="/d/e/f")

    tables = EntrySchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--type", "ICEBERG_TABLE"]), many=True
    )
    assert_that(tables).is_length(1)

    make_commit(table_id, iceberg_table, branch)
    logs = simplejson.loads(execute_cli_command(["--json", "log", branch]))

    assert len(logs) == 2

    # Now we commit a modified table
    make_commit(table_id, edited_iceberg_table, branch)

    tables = EntrySchema().loads(
        execute_cli_command(["--json", CONTENT_COMMAND, "list", "--ref", branch, "--type", "ICEBERG_TABLE"]), many=True
    )
    assert_that(tables).is_length(1)

    result_table = ContentSchema().loads(execute_cli_command(["--json", CONTENT_COMMAND, "view", "--ref", branch, table_id]), many=True)

    assert_that(result_table).is_length(1)
    edited_iceberg_table.id = result_table[0].id
    assert_that(result_table[0]).is_equal_to(edited_iceberg_table)


def _create_iceberg_table(
    metadata_location: str = "/a/b/c", snapshot_id: int = 42, schema_id: int = 42, spec_id: int = 42, sort_order_id: int = 42
) -> IcebergTable:
    return IcebergTable(None, metadata_location, snapshot_id, schema_id, spec_id, sort_order_id)


def _create_delta_lake_table(
    metadata_location_history: Optional[List[str]] = None,
    checkpoint_location_history: Optional[List[str]] = None,
    last_checkpoint: str = "x",
) -> DeltaLakeTable:
    if checkpoint_location_history is None:
        checkpoint_location_history = ["def"]
    if metadata_location_history is None:
        metadata_location_history = ["asd"]
    return DeltaLakeTable(None, last_checkpoint, checkpoint_location_history, metadata_location_history)


def _create_iceberg_view(
    metadata_location: str = "/a/b/c",
    version_id: int = 1,
    schema_id: int = 1,
    sql_text: str = "SELECT * FROM foo",
    dialect: str = "SPARK",
) -> IcebergView:
    return IcebergView(None, metadata_location, version_id, schema_id, dialect, sql_text)


def _get_head_branch_hash(branch: str) -> str:
    refs = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)}
    return refs[branch]
