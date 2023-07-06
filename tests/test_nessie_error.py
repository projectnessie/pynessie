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
"""Authentication tests for Nessie CLI."""

from pynessie.error import (
    NessieConflictException,
    NessieContentNotFoundException,
    NessieException,
    NessieNotFoundException,
    NessiePermissionException,
    NessiePreconidtionFailedException,
    NessieReferenceAlreadyExistsException,
    NessieReferenceConflictException,
    NessieReferenceNotFoundException,
    NessieServerException,
    NessieUnauthorizedException,
    _create_exception,
)


def test_raise_exception_missing_payload() -> None:
    """Test the handling error responses with missing JSON payload."""
    ex = _create_exception({}, 412, "reason123", "url123")
    assert isinstance(ex, NessiePreconidtionFailedException)
    assert "412" in str(ex.json())
    assert "reason123" in str(ex.json())
    assert "url123" in str(ex.json())

    ex = _create_exception({}, 401, "reason123", "url123")
    assert isinstance(ex, NessieUnauthorizedException)

    ex = _create_exception({}, 403, "reason123", "url123")
    assert isinstance(ex, NessiePermissionException)

    ex = _create_exception({}, 404, "reason123", "url123")
    assert isinstance(ex, NessieNotFoundException)

    ex = _create_exception({}, 409, "reason123", "url123")
    assert isinstance(ex, NessieConflictException)

    ex = _create_exception({}, 599, "reason123", "url123")
    assert isinstance(ex, NessieServerException)
    assert "599" in str(ex.json())
    assert "Server Error" in str(ex)
    assert "Server Error" in str(ex.json())
    assert "Internal Server Error" in str(ex.json())

    ex = _create_exception({}, 12345, "reason123", "url123")
    assert isinstance(ex, NessieException)
    assert "12345" in str(ex.json())


def _test_error_code(error_code: str, exception: type) -> None:
    error_dict = {
        "message": "msg123",
        "status": 499,
        "errorCode": error_code,
    }
    ex = _create_exception(error_dict, 498, "reason123", "url123")
    assert isinstance(ex, exception)
    ex_str = str(ex)
    assert "Client Error" in ex_str
    assert "msg123" in ex_str
    assert "reason123" in ex_str
    ex_str_json = str(ex.json())
    assert "Client Error" in ex_str_json
    assert "499" in ex_str_json
    assert "498" in ex_str_json
    assert "msg123" in ex_str_json
    assert "reason123" in ex_str_json
    assert "url123" in ex_str_json


def test_raise_ref_not_found() -> None:
    """Test the handling error code REFERENCE_NOT_FOUND."""
    _test_error_code("REFERENCE_NOT_FOUND", NessieReferenceNotFoundException)


def test_raise_content_not_found() -> None:
    """Test the handling error code CONTENT_NOT_FOUND."""
    _test_error_code("CONTENT_NOT_FOUND", NessieContentNotFoundException)


def test_raise_ref_conflict() -> None:
    """Test the handling error code REFERENCE_CONFLICT."""
    _test_error_code("REFERENCE_CONFLICT", NessieReferenceConflictException)


def test_raise_ref_already_exists() -> None:
    """Test the handling error code REFERENCE_ALREADY_EXISTS."""
    _test_error_code("REFERENCE_ALREADY_EXISTS", NessieReferenceAlreadyExistsException)
