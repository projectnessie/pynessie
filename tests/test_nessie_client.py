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
import pytest

from pynessie import init
from pynessie.client._endpoints import _sanitize_url
from pynessie.error import NessieConflictException
from pynessie.model import Branch, Entries


@pytest.mark.nessieserver
def test_client_interface_e2e() -> None:
    """Test client object against live server."""
    client = init()
    assert isinstance(client.get_base_url(), str)
    assert client.get_base_url().startswith("http://localhost:")
    assert client.get_base_url().endswith("/api/v1")
    references = client.list_references().references
    assert len(references) == 1
    assert references[0] == Branch("main", references[0].hash_)
    main_name = references[0].name
    main_commit = references[0].hash_
    with pytest.raises(NessieConflictException):
        client.create_branch("main", "main", client.get_reference(None).hash_)
    created_reference = client.create_branch("test", main_name, main_commit)
    created_reference_with_slash = client.create_branch("test/branch/name", main_name, main_commit)
    references = client.list_references().references
    assert len(references) == 3
    assert next(i for i in references if i.name == "main") == Branch("main", main_commit)
    assert next(i for i in references if i.name == "test") == Branch("test", main_commit)
    assert next(i for i in references if i.name == "test/branch/name") == Branch("test/branch/name", main_commit)
    # Testing multiple references
    ref_to_test = {
        "test": created_reference,
        "test/branch/name": created_reference_with_slash,
    }
    for ref_name, create_branch_ref in ref_to_test.items():
        reference = client.get_reference(ref_name)
        assert create_branch_ref == reference
        assert isinstance(reference.hash_, str)
        tables = client.list_keys(reference.name, reference.hash_)
        assert isinstance(tables, Entries)
        assert len(tables.entries) == 0
    assert isinstance(main_commit, str)
    client.delete_branch("test", main_commit)
    references = client.list_references().references
    assert len(references) == 2
    client.delete_branch("test/branch/name", main_commit)
    references = client.list_references().references
    assert len(references) == 1


def test_client_sanitize_url() -> None:
    """Test sanitization of URLs."""
    client = init()
    base_url = client.get_base_url()
    assert _sanitize_url(base_url) == base_url
    assert _sanitize_url(base_url + "/trees/tree") == base_url + "/trees/tree"
    assert _sanitize_url(base_url + "/trees/tree/{}", "my tag with spaces") == base_url + "/trees/tree/my%20tag%20with%20spaces"
    assert _sanitize_url(base_url + "/trees/tree/{}", "my/tag with mixed.types") == base_url + "/trees/tree/my%2Ftag%20with%20mixed.types"
    assert (
        _sanitize_url(base_url + "/trees/tree/{}/{}", "tag/name", "other/string@with-at")
        == base_url + "/trees/tree/tag%2Fname/other%2Fstring%40with-at"
    )
