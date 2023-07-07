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
"""Configure pytest."""

import os
import shutil
import tempfile
from typing import List, Optional

import attr
import pytest
import requests
from assertpy import assert_that
from click.testing import CliRunner, Result
from testcontainers.core.generic import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

from pynessie import cli
from pynessie.model import Content, ContentSchema, ReferenceSchema


class NessieContainer(DockerContainer):
    """Nessie test container."""

    NESSIE_PORT = 19120
    DEFAULT_IMAGE = "ghcr.io/projectnessie/nessie:latest"
    DEFAULT_TEST_IMAGE = "ghcr.io/projectnessie/nessie-unstable:latest"

    def __init__(self, image: str = DEFAULT_IMAGE) -> None:
        """Nessie test container constructor using the Nessie image tagged as 'latest'."""
        super().__init__(image=image)
        self.with_exposed_ports(NessieContainer.NESSIE_PORT)

    def get_base_url(self) -> str:
        """Get the root Nessie HTTP URL."""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(NessieContainer.NESSIE_PORT)
        return f"http://{host}:{port}/"

    def get_url(self, api_version: int = 1) -> str:
        """Retrieve the Nessie API URL for the given Nessie API version, defaults to 1."""
        return f"{self.get_base_url()}api/v{api_version}"

    @wait_container_is_ready(requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout)
    def _connect(self) -> None:
        response = requests.get(f"{self.get_base_url()}", timeout=1)
        response.raise_for_status()

    def start(self) -> "NessieContainer":
        """Starts the Nessie container, waits until Nessie is ready."""
        super().start()
        self._connect()
        return self


@attr.dataclass
class NessieTestConfig:
    """Test configs for pynessie tests."""

    config_dir: str
    cleanup: bool

    nessie_container: Optional[NessieContainer]


nessie_test_config: NessieTestConfig = NessieTestConfig("", False, None)


def pytest_configure(config):  # noqa
    """Configure pytest."""
    config.addinivalue_line("markers", "doc: mark as end-to-end test.")


def pytest_sessionstart(session: pytest.Session) -> None:
    """Setup a fresh temporary config directory for tests."""
    nessie_test_config.config_dir = tempfile.mkdtemp() + "/"
    # Instruct Confuse to keep Nessie config file in the temp location:
    os.environ["NESSIEDIR"] = nessie_test_config.config_dir


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Remove temporary config directory."""
    shutil.rmtree(nessie_test_config.config_dir)

    if nessie_test_config.nessie_container is not None:
        nessie_test_config.nessie_container.stop()


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Starts a Nessie container, if needed."""
    if nessie_test_config.nessie_container is None and item.get_closest_marker("nessieserver") is not None:
        image = os.environ["NESSIE_TEST_IMAGE"] if "NESSIE_TEST_IMAGE" in os.environ else NessieContainer.DEFAULT_TEST_IMAGE
        nessie_test_config.nessie_container = NessieContainer(image=image)
        nessie_test_config.nessie_container.start()

        os.environ["NESSIE_ENDPOINT"] = nessie_test_config.nessie_container.get_url(1)


def pytest_runtest_teardown(item: pytest.Item, nextitem: Optional[pytest.Item]) -> None:
    """Stops the Nessie container, if one was started."""
    if item.get_closest_marker("nessieserver") is not None and nessie_test_config.nessie_container is not None:
        reset_nessie_server_state()


def execute_cli_command_raw(args: List[str], input_data: Optional[str] = None, ret_val: int = 0) -> Result:
    """Execute a Nessie CLI command."""
    result = CliRunner().invoke(cli.cli, args, input=input_data)
    if result.exit_code != ret_val:
        print(f"Nessie CLI exited with unexpected code {result.exit_code}, expected {ret_val}")
        print(result.stdout)
        print(result.stderr)
        print(result)  # exception
    assert_that(result.exit_code).is_equal_to(ret_val)
    return result


def execute_cli_command(args: List[str], input_data: Optional[str] = None, ret_val: int = 0) -> str:
    """Execute a Nessie CLI command and return its STDOUT."""
    return execute_cli_command_raw(args, input_data=input_data, ret_val=ret_val).stdout


def ref_hash(ref: str) -> str:
    """Get the hash for a reference."""
    refs = ReferenceSchema().loads(execute_cli_command(["--json", "branch", "-l"]), many=True)
    return next(i.hash_ for i in refs if i.name == ref)


def make_commit(
    key: str, table: Content, branch: str, head_hash: Optional[str] = None, message: str = "test message", author: str = "nessie test"
) -> None:
    """Make commit through Nessie CLI."""
    if not head_hash:
        refs = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)}
        head_hash = refs[branch]
    assert isinstance(head_hash, str)
    execute_cli_command(
        ["content", "commit", "--stdin", key, "--ref", branch, "-m", message, "-c", head_hash, "--author", author],
        input_data=ContentSchema().dumps(table),
    )


def reset_nessie_server_state() -> None:
    """Resets the Nessie Server to an initial, clean state for testing."""
    # Delete all branches except main
    branches = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    for branch in branches:
        if branch.name != "main":
            execute_cli_command(["branch", "-d", branch.name])

    # Delete all tags
    tags = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    for tag in tags:
        execute_cli_command(["tag", "-d", tag.name])

    # Note: This hash should match the java constant AbstractDatabaseAdapter.NO_ANCESTOR
    no_ancestor_hash = "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d"

    # Reset the main branch to the "root" (a.k.a. no ancestor) hash
    execute_cli_command(["branch", "--force", "-o", no_ancestor_hash, "main", "main"])

    # Verify that the main branch has been reset
    branches = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert_that(branches).is_length(1)
    assert_that(branches[0].name).is_equal_to("main")
    assert_that(branches[0].hash_).is_equal_to(no_ancestor_hash)
