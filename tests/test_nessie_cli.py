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
#
"""Tests for `pynessie` package."""
import itertools
from typing import List

import confuse
import pytest
import simplejson
from assertpy import assert_that

from pynessie import __version__
from pynessie.model import (
    Branch,
    CommitMeta,
    CommitMetaSchema,
    ContentKey,
    ContentSchema,
    DiffResponseSchema,
    EntrySchema,
    IcebergTable,
    LogEntry,
    LogEntrySchema,
    MergeResponseSchema,
    ReferenceSchema,
)

from .conftest import (
    execute_cli_command,
    execute_cli_command_raw,
    make_commit,
    ref_hash,
)


@pytest.mark.nessie
def test_command_line_interface() -> None:
    """Test the CLI."""
    assert "Usage: nessie" in execute_cli_command([])
    assert "Usage: nessie" in execute_cli_command(["--help"])
    assert __version__ in execute_cli_command(["--version"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch", "-l"]), many=True)
    assert len(references) == 1
    assert references[0].name == "main"
    assert isinstance(references[0], Branch)


def test_config_options() -> None:
    """Ensure config cli option is consistent."""
    assert "Usage: nessie" in execute_cli_command(["config"])
    vars_to_add = ["--add x", "--get x", "--list", "--unset x"]
    for i in itertools.permutations(vars_to_add, 2):
        assert "Error: Illegal usage: " in execute_cli_command(["config"] + [*i[0].split(" "), *i[1].split(" ")], ret_val=2)

    execute_cli_command(["config", "x", "--add", "x"])


def test_set_unset() -> None:
    """Test config set/unset/list."""
    execute_cli_command(["config", "--add", "test.data", "123", "--type", "int"])
    assert execute_cli_command(["config", "test.data", "--type", "int"]) == "123\n"
    execute_cli_command(["config", "--unset", "test.data"])
    assert "123" not in execute_cli_command(["config", "--list"])


@pytest.mark.nessie
def test_remote() -> None:
    """Test setting and viewing remote."""
    execute_cli_command(["remote", "add", "http://test.url"])
    execute_cli_command(["remote", "add", "http://localhost:19120/api/v1"])
    assert "main" in execute_cli_command(["--json", "remote", "show"])
    execute_cli_command(["remote", "set-head", "dev"])
    assert execute_cli_command(["config", "default_branch"]) == "dev\n"
    execute_cli_command(["remote", "set-head", "dev", "-d"])
    result = execute_cli_command_raw(["config", "default_branch"], ret_val=1)
    assert result.stdout == ""
    assert isinstance(result.exception, confuse.exceptions.ConfigTypeError)


def _new_table() -> IcebergTable:
    return IcebergTable(None, "/a/b/c", 42, 43, 44, 45)


@pytest.mark.nessie
def test_log() -> None:
    """Test log and log filtering."""
    main_hash = ref_hash("main")

    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    assert len(logs) == 0
    execute_cli_command(["branch", "dev_test_log"])
    table = _new_table()
    make_commit("log_foo_dev", table, "dev_test_log", author="nessie_user1")
    table = _new_table()
    make_commit("log_foo_bar", table, "main", author="nessie_user1", message="commit to main")
    tables = ContentSchema().loads(execute_cli_command(["--json", "content", "view", "log_foo_bar"]), many=True)
    assert len(tables) == 1
    table.id = tables[0].id
    assert tables[0] == table

    ext_logs: List[LogEntry] = LogEntrySchema().loads(execute_cli_command(["--json", "log", "-x"]), many=True)
    assert (
        len(ext_logs) == 1
        and ext_logs[0].commit_meta.message == "commit to main"
        and ext_logs[0].commit_meta.author == "nessie_user1"
        and ext_logs[0].parent_commit_hash is not None
        and len(ext_logs[0].operations) == 1
        and ext_logs[0].operations[0].key == ContentKey.from_path_string("log_foo_bar")
    )

    simple_logs: List[CommitMeta] = CommitMetaSchema().loads(execute_cli_command(["--json", "log"]), many=True)
    assert len(simple_logs) == 1 and simple_logs[0].message == "commit to main" and simple_logs[0].author == "nessie_user1"

    logs_by_hash: List[CommitMeta] = CommitMetaSchema().loads(execute_cli_command(["--json", "log", simple_logs[0].hash_]), many=True)
    assert_that(logs_by_hash).is_equal_to(simple_logs)

    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    logs_hash = logs[0]["hash"]
    assert_that(logs).is_length(1)
    assert_that(simplejson.loads(execute_cli_command(["--json", "log", "--revision-range", logs_hash]))).is_equal_to(logs)
    assert_that(simplejson.loads(execute_cli_command(["--json", "log", "--revision-range", f"..{logs_hash}"]))).is_equal_to(logs)
    assert_that(simplejson.loads(execute_cli_command(["--json", "log", f"main@{logs_hash}", "--revision-range", logs_hash]))).is_equal_to(
        logs
    )

    entries = EntrySchema().loads(execute_cli_command(["--json", "content", "list"]), many=True)
    assert_that(entries).is_length(1)
    execute_cli_command(
        [
            "--json",
            "content",
            "commit",
            "log_foo_bar",
            "-R",
            "--ref",
            "main",
            "-m",
            "delete_message",
            "-c",
            logs[0]["hash"],
            "--author",
            "nessie_user2",
        ],
    )
    assert_that(simplejson.loads(execute_cli_command(["--json", "log", "-n", "1"]))).is_length(1)
    assert_that(simplejson.loads(execute_cli_command(["--json", "log", "dev_test_log"]))).is_length(1)
    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    assert_that(logs).is_length(2)
    assert_that(
        simplejson.loads(execute_cli_command(["--json", "log", "--revision-range", "{}..{}".format(logs[0]["hash"], logs[1]["hash"])]))
    ).is_length(1)
    assert_that(simplejson.loads(execute_cli_command(["--json", "log"]))).is_length(2)
    logs = simplejson.loads(execute_cli_command(["--json", "log", "--author", "nessie_user1"]))
    assert_that(logs).is_length(1)
    assert_that(logs[0]["author"]).is_equal_to("nessie_user1")
    logs = simplejson.loads(execute_cli_command(["--json", "log", "--author", "nessie_user2"]))
    assert_that(logs).is_length(1)
    assert_that(logs[0]["author"]).is_equal_to("nessie_user2")
    assert_that(simplejson.loads(execute_cli_command(["--json", "log", "--author", "nessie_user2", "--author", "nessie_user1"]))).is_length(
        2
    )
    # the committer is set on the server-side and is empty if we're not logged
    # in when performing a commit
    assert_that(simplejson.loads(execute_cli_command(["--json", "log", "--committer", ""]))).is_length(2)
    assert_that(
        simplejson.loads(
            execute_cli_command(["--json", "log", "--filter", "commit.author == 'nessie_user2' || commit.author == 'non_existing'"])
        )
    ).is_length(1)
    assert_that(
        simplejson.loads(
            execute_cli_command(["--json", "log", "--after", "2001-01-01T00:00:00+00:00", "--before", "2999-12-30T23:00:00+00:00"])
        )
    ).is_length(2)

    # Specifying a different end-hash via revision-range and ref is forbidden
    execute_cli_command(["--json", "log", main_hash, "--revision-range", logs[0]["hash"]], ret_val=2)
    execute_cli_command(["--json", "log", f"main@{main_hash}", "--revision-range", logs[0]["hash"]], ret_val=2)


@pytest.mark.nessie
def test_branch() -> None:
    """Test create and assign refs."""
    main_hash = ref_hash("main")

    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert len(references) == 1
    execute_cli_command(["branch", "dev"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert len(references) == 2
    execute_cli_command(["branch", "etl", "main"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert len(references) == 3
    execute_cli_command(["branch", "dev_hash", main_hash])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert len(references) == 4
    execute_cli_command(["branch", "etl_hash", f"main@{main_hash}"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert len(references) == 5
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch", "-l", "etl"]), many=False)
    assert_that(references.name).is_equal_to("etl")
    references = simplejson.loads(execute_cli_command(["--json", "branch", "-l", "foo"]))
    assert len(references) == 0

    table = _new_table()
    make_commit("test_branch_metadata", table, "dev", author="nessie_user1")

    branch = ReferenceSchema().loads(execute_cli_command(["--json", "branch", "-l", "dev", "--extended"]))
    ref_metadata = branch.metadata
    assert_that(ref_metadata).is_not_none()
    assert_that(ref_metadata.num_commits_ahead).is_equal_to(1)
    assert_that(ref_metadata.num_commits_behind).is_equal_to(0)
    assert_that(ref_metadata.num_total_commits).is_equal_to(1)
    assert_that(ref_metadata.common_ancestor_hash).is_not_empty()
    assert_that(ref_metadata.commit_meta_of_head).is_not_none()

    dev_hash = ref_hash("dev")

    execute_cli_command(["branch", "-d", "etl"])
    execute_cli_command(["branch", "-d", "dev", "-c", dev_hash])
    execute_cli_command(["branch", "-d", "etl_hash", "-c", main_hash])
    execute_cli_command(["branch", "-d", "dev_hash"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert len(references) == 1


@pytest.mark.nessie
def test_tag() -> None:
    """Test create and assign refs."""
    main_hash = ref_hash("main")

    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    assert len(references) == 0
    execute_cli_command(["tag", "dev-tag", "main"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    assert len(references) == 1
    execute_cli_command(["tag", "etl-tag", "main"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    assert len(references) == 2
    execute_cli_command(["tag", "dev-hash-tag", main_hash])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    assert len(references) == 3
    execute_cli_command(["tag", "etl-hash-tag", f"main@{main_hash}"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    assert len(references) == 4
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag", "-l", "etl-tag"]), many=False)
    assert_that(references.name).is_equal_to("etl-tag")
    references = simplejson.loads(execute_cli_command(["--json", "tag", "-l", "foo"]))
    assert len(references) == 0
    execute_cli_command(["tag", "-d", "etl-tag"])
    execute_cli_command(["tag", "-d", "etl-hash-tag"])
    execute_cli_command(["tag", "-d", "dev-tag", "-c", main_hash])
    execute_cli_command(["tag", "-d", "dev-hash-tag", "-c", main_hash])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    assert len(references) == 0
    execute_cli_command(["tag", "v1.0"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)}
    branches = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)}
    assert tags["v1.0"] == branches["main"]

    execute_cli_command(["branch", "metadata_branch", "main"])
    table = _new_table()
    make_commit("test_tag_metadata", table, "metadata_branch", author="nessie_user1")
    execute_cli_command(["tag", "metadata_tag", "metadata_branch"])
    ref = ReferenceSchema().loads(execute_cli_command(["--json", "tag", "-l", "metadata_tag", "--extended"]))
    ref_metadata = ref.metadata
    assert_that(ref_metadata).is_not_none()
    assert_that(ref_metadata.num_commits_ahead).is_none()
    assert_that(ref_metadata.num_commits_behind).is_none()
    assert_that(ref_metadata.num_total_commits).is_equal_to(1)
    assert_that(ref_metadata.common_ancestor_hash).is_none()
    assert_that(ref_metadata.commit_meta_of_head).is_not_none()


@pytest.mark.nessie
def test_assign() -> None:
    """Test assign operation."""
    execute_cli_command(["branch", "dev"])
    make_commit("assign_foo_bar", _new_table(), "dev")
    execute_cli_command(["branch", "main", "dev", "--force"])
    branches = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    refs = {i.name: i.hash_ for i in branches}
    assert refs["main"] == refs["dev"]
    execute_cli_command(["tag", "v1.0", "main"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)}
    assert tags["v1.0"] == refs["main"]
    execute_cli_command(["tag", "v1.0", "dev", "--force"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)}
    assert tags["v1.0"] == refs["dev"]


@pytest.mark.nessie
def test_merge() -> None:
    """Test merge operation."""
    make_commit("initial_commit", _new_table(), "main", message="Initial commit")
    initial_hash = ref_hash("main")
    execute_cli_command(["branch", "dev"])
    make_commit("merge_foo_bar", _new_table(), "dev")
    main_hash = ref_hash("main")
    dev_hash = ref_hash("dev")

    merge_output = execute_cli_command(["merge", "dev", "-c", main_hash])

    branches = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    refs = {i.name: i.hash_ for i in branches}

    expected_output_list = [
        f"Merged dev onto main (was on {main_hash} before merge)",
        f"Identified merge base commit {initial_hash}",
        f"Resultant hash on main after merge: {refs['main']}",
    ]
    assert_that(merge_output.splitlines()).is_equal_to(expected_output_list)

    print(merge_output.splitlines())

    # If we try to merge again from dev to main we get an error.
    # This is because there is nothing more to merge.
    merge_output = execute_cli_command(["merge", "dev", "-c", refs["main"]])
    expected_output_list = [
        f"Nothing merged from dev onto main (still on {refs['main']})",
        f"Identified merge base commit {dev_hash}",
        f"Current, unchanged hash on main after merge: {refs['main']}",
    ]
    assert_that(merge_output.splitlines()).is_equal_to(expected_output_list)

    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    # we don't check for equality of hashes here because a merge
    # produces a different commit hash on the target branch
    assert_that(logs).is_length(2)
    assert_that(logs[0]["message"]).starts_with("Merged dev at ")
    logs = simplejson.loads(execute_cli_command(["--json", "log", "dev"]))
    assert_that(logs).is_length(2)


@pytest.mark.nessie
def test_merge_json() -> None:
    """Test merge operation."""
    make_commit("initial_commit", _new_table(), "main", message="Initial commit")
    execute_cli_command(["branch", "dev"])
    make_commit("merge_foo_bar", _new_table(), "dev")
    main_hash = ref_hash("main")
    dev_hash = ref_hash("dev")

    # Passing detached commit-id plus a _different_ hash-on-ref --> error
    execute_cli_command(["merge", f"dev@{dev_hash}", "-c", main_hash, "-o", main_hash], ret_val=1)

    merge_output = execute_cli_command(["--json", "merge", "dev", "-c", main_hash])
    merge_response = MergeResponseSchema().loads(merge_output)

    # Check merge response
    assert_that(merge_response.target_branch).is_equal_to("main")
    assert_that(merge_response.expected_hash).is_equal_to(main_hash)
    assert_that(merge_response.effective_target_hash).is_equal_to(main_hash)
    assert_that(merge_response.details).is_length(1)

    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    # we don't check for equality of hashes here because a merge
    # produces a different commit hash on the target branch
    assert_that(logs).is_length(2)
    assert_that(logs[0]["message"]).starts_with("Merged dev at ")
    logs = simplejson.loads(execute_cli_command(["--json", "log", "dev"]))
    assert_that(logs).is_length(2)


@pytest.mark.nessie
def test_merge_detached() -> None:
    """Test merge operation."""
    make_commit("initial_commit", _new_table(), "main", message="Initial commit")
    initial_hash = ref_hash("main")
    execute_cli_command(["branch", "dev"])
    make_commit("merge_foo_bar", _new_table(), "dev")
    main_hash = ref_hash("main")
    dev_hash = ref_hash("dev")

    # Passing detached commit-id plus a _different_ hash-on-ref --> error
    execute_cli_command(["merge", dev_hash, "-c", main_hash, "-o", main_hash], ret_val=1)

    merge_output = execute_cli_command(["merge", "dev", "-c", main_hash])

    branches = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    refs = {i.name: i.hash_ for i in branches}

    expected_output_list = [
        f"Merged dev onto main (was on {main_hash} before merge)",
        f"Identified merge base commit {initial_hash}",
        f"Resultant hash on main after merge: {refs['main']}",
    ]
    assert_that(merge_output.splitlines()).is_equal_to(expected_output_list)

    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    # we don't check for equality of hashes here because a merge
    # produces a different commit hash on the target branch
    assert_that(logs).is_length(2)
    assert_that(logs[0]["message"]).starts_with("Merged dev at ")
    logs = simplejson.loads(execute_cli_command(["--json", "log", "dev"]))
    assert_that(logs).is_length(2)


@pytest.mark.nessie
def test_merge_detached_json() -> None:
    """Test merge operation."""
    make_commit("initial_commit", _new_table(), "main", message="Initial commit")
    execute_cli_command(["branch", "dev"])
    make_commit("merge_foo_bar", _new_table(), "dev")
    main_hash = ref_hash("main")
    dev_hash = ref_hash("dev")

    # Passing detached commit-id plus a _different_ hash-on-ref --> error
    execute_cli_command(["merge", dev_hash, "-c", main_hash, "-o", main_hash], ret_val=1)

    merge_output = execute_cli_command(["--json", "merge", dev_hash, "-c", main_hash])
    merge_response = MergeResponseSchema().loads(merge_output)

    # Check merge response
    assert_that(merge_response.target_branch).is_equal_to("main")
    assert_that(merge_response.expected_hash).is_equal_to(main_hash)
    assert_that(merge_response.effective_target_hash).is_equal_to(main_hash)
    assert_that(merge_response.details).is_length(1)

    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    # we don't check for equality of hashes here because a merge
    # produces a different commit hash on the target branch
    assert_that(logs).is_length(2)
    assert_that(logs[0]["message"]).starts_with("Merged DETACHED at ")
    logs = simplejson.loads(execute_cli_command(["--json", "log", "dev"]))
    assert_that(logs).is_length(2)


@pytest.mark.nessie
def test_transplant() -> None:
    """Test transplant operation."""
    execute_cli_command(["branch", "dev"])
    make_commit("transplant_foo_bar", _new_table(), "dev", message="commit 1")
    make_commit("bar_bar", _new_table(), "dev", message="commit 2")
    make_commit("foo_baz", _new_table(), "dev", message="commit 3")
    main_hash = ref_hash("main")
    logs = simplejson.loads(execute_cli_command(["--json", "log", "dev"]))
    first_hash = [i["hash"] for i in logs]
    execute_cli_command(["cherry-pick", "-c", main_hash, "-s", "dev", first_hash[1], first_hash[0]])

    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    assert_that(logs).is_length(2)  # two commits were transplanted into an empty `main`
    assert_that(logs[0]["message"]).is_equal_to("commit 3")
    assert_that(logs[1]["message"]).is_equal_to("commit 2")


@pytest.mark.nessie
def test_diff() -> None:
    """Test log and log filtering."""
    diff = DiffResponseSchema().loads(execute_cli_command(["--json", "diff", "main", "main"]))
    main_hash = ref_hash("main")
    assert_that(diff).is_not_none()
    assert_that(diff.diffs).is_empty()
    branch = "dev_test_diff"
    execute_cli_command(["branch", branch])
    table = _new_table()
    content_key = "diff_foo_dev"
    make_commit(content_key, table, branch, author="nessie_user1")
    branch_hash = ref_hash(branch)

    diff = DiffResponseSchema().loads(execute_cli_command(["--json", "diff", "main", branch]))
    assert_that(diff).is_not_none()
    assert_that(diff.diffs).is_length(1)
    diff_entry = diff.diffs[0]
    assert_that(diff_entry.content_key).is_equal_to(ContentKey.from_path_string(content_key))
    assert_that(diff_entry.from_content).is_none()
    assert_that(diff_entry.to_content).is_not_none()
    table.id = diff_entry.to_content.id
    assert_that(diff_entry.to_content).is_equal_to(table)

    diff_detached = DiffResponseSchema().loads(execute_cli_command(["--json", "diff", main_hash, branch_hash]))
    assert_that(diff_detached).is_equal_to(diff)
    diff_detached = DiffResponseSchema().loads(execute_cli_command(["--json", "diff", f"DETACHED@{main_hash}", f"DETACHED@{branch_hash}"]))
    assert_that(diff_detached).is_equal_to(diff)
    diff_detached = DiffResponseSchema().loads(execute_cli_command(["--json", "diff", f"main@{main_hash}", f"{branch}@{branch_hash}"]))
    assert_that(diff_detached).is_equal_to(diff)
