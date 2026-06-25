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

"""Direct API operations on Nessie with requests."""

import os
from typing import Any, Optional, Union, cast
from ._endpoints import _post, _get, _delete, _put, _sanitize_timeout

import simplejson as jsonlib
from requests.auth import AuthBase
from requests.exceptions import HTTPError

from pynessie.error import _create_exception
from pynessie.model import ContentKey


## GET to /v2/config
def get_config_v2(
    base_url: str,
    auth: Optional[AuthBase],
    ssl_verify: bool = True,
    timeout_sec: Optional[int] = None
) -> dict:
    """
    Get the configuration details.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param ssl_verify: ignore ssl errors if False
    :param timeout_sec: number of seconds before the request times out
    :return: json config data
    """
    timeout_sec = _sanitize_timeout(timeout_sec)
    return cast(dict, _get(base_url + "/config", auth, ssl_verify, timeout_sec=timeout_sec))


## GET to /v2/trees
def all_references_v2(
    base_url: str,
    auth: Optional[AuthBase],
    fetch: Optional[str] = None,
    filter: Optional[str] = None,
    max_records: Optional[int] = None,
    page_token: Optional[str] = None,
    ssl_verify: bool = True,
    timeout_sec: Optional[int] = None
) -> dict:
    """
    Get information about all branches and tags in the Nessie repository.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param fetch: Specifies how much extra information is to be retrieved from the server.
    :param filter: A Common Expression Language (CEL) expression to filter the results.
    :param max_records: Maximum number of entries to return.
    :param page_token: Paging continuation token.
    :param ssl_verify: ignore ssl errors if False
    :param timeout_sec: number of seconds before the request times out
    :return: json config data
    """
    params = {
        "fetch": fetch,
        "filter": filter,
        "max-records": max_records,
        "page-token": page_token
    }

    # Remove any parameters that weren't provided
    params = {k: v for k, v in params.items() if v is not None}

    timeout_sec = _sanitize_timeout(timeout_sec)
    return cast(dict, _get(base_url + "/trees", auth, ssl_verify, params=params, timeout_sec=timeout_sec))

## Post to /v2/trees
def create_reference_v2(
    base_url: str,
    auth: Optional[AuthBase],
    name: str,
    ref_type: str = "BRANCH",
    source_reference: dict = {"name":"main", "type":"BRANCH"},
    ssl_verify: bool = True
) -> dict:
    """
    Create a new branch or tag in the Nessie repository.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param name: Name of the new branch or tag
    :param ref_type: Type of the new reference ('BRANCH' or 'TAG')
    :param source_reference: Source reference data (should be a dictionary representing the reference object)
    :param ssl_verify: ignore ssl errors if False
    :return: json Nessie branch or tag
    """
    params = {
        "name": name,
        "type": ref_type.upper()
    }

    ref_json = jsonlib.dumps(source_reference)

    return cast(dict, _post(base_url + "/trees", auth, ref_json, ssl_verify=ssl_verify, params=params))


## Get to /v2/trees/{ref}
def get_referencev2(
    ref: str,
    base_url: str,
    auth: Optional[AuthBase],
    fetch: Optional[str] = None,
    ssl_verify: bool = True,
    timeout_sec: Optional[int] = None
) -> dict:
    """
    Get the contents of a reference.

    :param ref: Name of the reference (branch or tag)
    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param fetch: Specifies how much extra information is to be retrieved from the server.
    :param ssl_verify: ignore ssl errors if False
    :param timeout_sec: number of seconds before the request times out
    :return: json of the reference's content
    """
    params = {}
    if fetch:
        params['fetch'] = fetch

    timeout_sec = _sanitize_timeout(timeout_sec)
    return cast(dict, _get(base_url + f"/trees/{ref}", auth, ssl_verify, params=params, timeout_sec=timeout_sec))

## Function for Getting the Latest Hash of a Reference
def get_hash(
    name: str,
    base_url: str,
    auth: Optional[AuthBase],
    ssl_verify: bool = True,
    timeout_sec: Optional[int] = None
) -> str:
    """
    Get the hash of a branch or tag.

    :param name: Name of the branch or tag
    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param ssl_verify: ignore ssl errors if False
    :param timeout_sec: number of seconds before the request times out
    :return: Hash of the branch or tag
    """
    timeout_sec = _sanitize_timeout(timeout_sec)
    response = get_referencev2(name, base_url, auth, ssl_verify, timeout_sec)

    # Extract the hash from the response
    hash_value = response["reference"]["hash"]

    return hash_value


## Post to /v2/trees/{branch@hash}/history/commit
def commitv2(
    operations: dict,
    base_url: str,
    auth: Optional[AuthBase],
    branch: str = "main",
    hash: Optional[str] = None,
    ssl_verify: bool = True,
    timeout_sec: Optional[int] = None
) -> dict:
    """
    Create a new commit on a branch.

    :param operations: operations to be committed
    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param branch: Name of the branch to commit on. Default is "main".
    :param hash: Hash of the commit. If not provided, the hash of the 'branch' will be used.
    :param ssl_verify: ignore ssl errors if False
    :param timeout_sec: number of seconds before the request times out
    :return: json of the server's response
    """
    hash = hash or get_hash(branch, base_url, auth, ssl_verify, timeout_sec)
    url = f"{base_url}/trees/{branch}@{hash}/history/commit"
    payload = jsonlib.dumps(operations)

    timeout_sec = _sanitize_timeout(timeout_sec)
    return cast(dict, _post(url, auth, payload, ssl_verify=ssl_verify, timeout_sec=timeout_sec))


## Post to /v2/trees/{branch@hash}/history/merge
def mergev2(
    merge: dict,
    base_url: str,
    auth: Optional[AuthBase],
    branch: str = "main",
    hash: Optional[str] = None,
    ssl_verify: bool = True,
    timeout_sec: Optional[int] = None
) -> dict:
    """
    Create a merge on a branch.

    :param merge: merge operation to be performed
    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param branch: Name of the branch to merge on. Default is "main".
    :param hash: Hash of the commit. If not provided, the hash of the 'branch' will be used.
    :param ssl_verify: ignore ssl errors if False
    :param timeout_sec: number of seconds before the request times out
    :return: json of the server's response
    """
    hash = hash or get_hash(branch, base_url, auth, ssl_verify, timeout_sec)
    url = f"{base_url}/trees/{branch}@{hash}/history/merge"
    payload = jsonlib.dumps(merge)

    timeout_sec = _sanitize_timeout(timeout_sec)
    return cast(dict, _post(url, auth, payload, ssl_verify=ssl_verify, timeout_sec=timeout_sec))


## post to /v2/trees/{branch@hash}/history/transplant
def cherry_pickv2(
    transplant: dict,
    base_url: str,
    auth: Optional[AuthBase],
    branch: str = "main",
    hash: Optional[str] = None,
    ssl_verify: bool = True,
    timeout_sec: Optional[int] = None
) -> dict:
    """
    Create a transplant on a branch.

    :param transplant: transplant operation to be performed
    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param branch: Name of the branch to perform the transplant on. Default is "main".
    :param hash: Hash of the commit. If not provided, the hash of the 'branch' will be used.
    :param ssl_verify: ignore ssl errors if False
    :param timeout_sec: number of seconds before the request times out
    :return: json of the server's response
    """
    hash = hash or get_hash(branch, base_url, auth, ssl_verify, timeout_sec)
    url = f"{base_url}/trees/{branch}@{hash}/history/transplant"
    payload = jsonlib.dumps(transplant)

    timeout_sec = _sanitize_timeout(timeout_sec)
    return cast(dict, _post(url, auth, payload, ssl_verify=ssl_verify, timeout_sec=timeout_sec))



## Get to /trees/{from_ref}/diff/{to_ref}
def get_diffv2(
    from_ref: str,
    to_ref: str,
    base_url: str,
    auth: Optional[AuthBase],
    filter: Optional[str] = None,
    key: Optional[list] = None,
    max_key: Optional[str] = None,
    max_records: Optional[int] = None,
    min_key: Optional[str] = None,
    page_token: Optional[str] = None,
    prefix_key: Optional[str] = None,
    ssl_verify: bool = True,
    timeout_sec: Optional[int] = None
) -> dict:
    """
    Get differences between two references.

    :param from_ref: the reference from which to compare
    :param to_ref: the reference to compare to
    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param filter: Common Expression Language (CEL) expression
    :param key: a list of specific keys to fetch
    :param max_key: get records with keys less than this, in lexicographically ascending order
    :param max_records: limit for number of records to retrieve
    :param min_key: get records with keys greater than this, in lexicographically ascending order
    :param page_token: fetch page by token
    :param prefix_key: filter records by key prefix
    :param ssl_verify: ignore ssl errors if False
    :param timeout_sec: number of seconds before the request times out
    :return: json of the server's response
    """
    url = f"{base_url}/trees/{from_ref}/diff/{to_ref}"

    params = {
        "filter": filter,
        "key": key,
        "max-key": max_key,
        "max-records": max_records,
        "min-key": min_key,
        "page-token": page_token,
        "prefix-key": prefix_key,
    }

    params = {k: v for k, v in params.items() if v is not None}

    timeout_sec = _sanitize_timeout(timeout_sec)
    return cast(dict, _get(url, auth, params, ssl_verify=ssl_verify, timeout_sec=timeout_sec))

## Post to /trees/{ref}/
def assignv2(
    base_url: str,
    auth: Optional[AuthBase],
    ref: str,
    body: dict,
    ref_type: Optional[str] = None,
    ssl_verify: bool = True
) -> None:
    """
    Set the hash for a reference to the hash of another reference.

    :param base_url: base url
    :param auth: Authentication settings
    :param ref: name of the reference
    :param body: hash to become the new HEAD of the reference and the name of the reference via which that hash is reachable
    :param ref_type: type of the reference (optional)
    :param ssl_verify: ignore ssl errors if False
    """
    url = f"/trees/{ref}"
    params = {"type": ref_type} if ref_type else {}

    return _put(base_url + url, auth, body, ssl_verify=ssl_verify, params=params)

## Delete to /trees/{ref}@{ref_hash}
def delete_referencev2(
    base_url: str,
    auth: Optional[AuthBase],
    ref: str,
    ref_hash: str,
    ref_type: Optional[str] = "BRANCH",
    ssl_verify: bool = True
) -> None:
    """
    Delete a reference.

    :param base_url: base url
    :param auth: Authentication settings
    :param ref: name of the reference
    :param ref_hash: hash of the reference
    :param ref_type: type of the reference (optional, default is 'BRANCH')
    :param ssl_verify: ignore ssl errors if False
    """
    url = f"/trees/{ref}@{ref_hash}"
    params = {"type": ref_type} if ref_type else {}

    return _delete(base_url + url, auth, ssl_verify=ssl_verify, params=params)


## Get to /trees/{ref}/contents
def get_contentsv2(
    base_url: str,
    auth: Optional[AuthBase],
    ref: str,
    keys: list[str],
    with_doc: bool = False,
    ssl_verify: bool = True
) -> Union[dict, list]:
    """
    Get the contents of a reference using GET request.

    :param base_url: base url
    :param auth: Authentication settings
    :param ref: name of the reference
    :param keys: list of keys
    :param with_doc: flag to include doc in the response (optional, default is False)
    :param ssl_verify: ignore ssl errors if False
    :return: JSON response
    """
    url = f"/trees/{ref}/contents"
    params = {
        "key": keys,
        "with-doc": with_doc
    }

    return cast(dict,_get(base_url + url, auth, ssl_verify=ssl_verify, params=params))


## Post to /trees/{ref}/contents
def get_contentsv2_post(
    base_url: str,
    auth: Optional[AuthBase],
    ref: str,
    keys: list[str],
    with_doc: bool = False,
    ssl_verify: bool = True
) -> Union[dict, list]:
    """
    Get the contents of a reference using POST request.

    :param base_url: base url
    :param auth: Authentication settings
    :param ref: name of the reference
    :param keys: list of keys
    :param with_doc: flag to include doc in the response (optional, default is False)
    :param ssl_verify: ignore ssl errors if False
    :return: JSON response
    """
    url = f"/trees/{ref}/contents"
    params = {
        "with-doc": with_doc
    }
    payload = {
        "keys": keys
    }

    return cast(dict ,_post(base_url + url, auth, json=payload, ssl_verify=ssl_verify, params=params))



## Get to /trees/{ref}/contents/{key}
def get_contentv2(
    base_url: str,
    auth: Optional[AuthBase],
    ref: str,
    key: str,
    with_doc: bool = False,
    ssl_verify: bool = True
) -> Union[dict, list]:
    """
    Get the content of a reference.

    :param base_url: base url
    :param auth: Authentication settings
    :param ref: name of the reference
    :param key: the key
    :param with_doc: flag to include doc in the response (optional, default is False)
    :param ssl_verify: ignore ssl errors if False
    :return: JSON response
    """
    url = f"/trees/{ref}/contents/{key}"
    params = {
        "with-doc": with_doc
    }

    return _get(base_url + url, auth, ssl_verify=ssl_verify, params=params)

## Get to /trees/{ref}/entries
def list_tablesv2(
    base_url: str,
    auth: Optional[AuthBase],
    ref: str,
    content: Optional[bool] = None,
    filter: Optional[str] = None,
    key: Optional[str] = None,
    max_key: Optional[str] = None,
    max_records: Optional[int] = None,
    min_key: Optional[str] = None,
    page_token: Optional[str] = None,
    prefix_key: Optional[str] = None,
    ssl_verify: bool = True
) -> Union[dict, list]:
    """
    Get entries from a reference.

    :param base_url: base url
    :param auth: Authentication settings
    :param ref: name of the reference
    :param content: fetch content of key along with key metadata
    :param filter: apply a filter to keys
    :param key: find the specified key
    :param max_key: keys returned will be less than the max_key
    :param max_records: maximum records to return
    :param min_key: keys returned will be greater than or equal to min_key
    :param page_token: token to continue fetching results from a previous request
    :param prefix_key: keys returned will be prefixed with prefix_key
    :param ssl_verify: ignore ssl errors if False
    :return: JSON response
    """
    url = f"/trees/{ref}/entries"
    params = {
        "content": content,
        "filter": filter,
        "key": key,
        "max-key": max_key,
        "max-records": max_records,
        "min-key": min_key,
        "page-token": page_token,
        "prefix-key": prefix_key,
    }

    params = {k: v for k, v in params.items() if v is not None}

    return _get(base_url + url, auth, ssl_verify=ssl_verify, params=params)



## Get to /trees/{ref}/history
def reflogv2(
    base_url: str,
    auth: Optional[AuthBase],
    ref: str,
    fetch: Optional[bool] = None,
    filter: Optional[str] = None,
    limit_hash: Optional[str] = None,
    max_records: Optional[int] = None,
    page_token: Optional[str] = None,
    ssl_verify: bool = True
) -> Union[dict, list]:
    """
    Get commit log of a reference.

    :param base_url: base url
    :param auth: Authentication settings
    :param ref: name of the reference
    :param fetch: whether or not to fetch commit data
    :param filter: apply a filter to the commit log
    :param limit_hash: limit the log to descendants of the commit
    :param max_records: maximum records to return
    :param page_token: token to continue fetching results from a previous request
    :param ssl_verify: ignore ssl errors if False
    :return: JSON response
    """
    url = f"/trees/{ref}/history"
    params = {
        "fetch": fetch,
        "filter": filter,
        "limit-hash": limit_hash,
        "max-records": max_records,
        "page-token": page_token,
    }

    params = {k: v for k, v in params.items() if v is not None}

    return _get(base_url + url, auth, ssl_verify=ssl_verify, params=params)
