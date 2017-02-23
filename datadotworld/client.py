"""
data.world-py
Copyright 2017 data.world, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the
License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

This product includes software developed at data.world, Inc.(http://www.data.world/).
"""

import os
import re

import requests

from datadotworld._rest import ApiClient
from datadotworld._rest import DatasetsApi
from datadotworld._rest import UploadsApi

from .query import Results


class DataDotWorld:
    """A Python Client for Accessing data.world"""

    def __init__(self, token=None, propsfile="~/.data.world",
                 protocol="https",
                 query_host="query.data.world", api_host="api.data.world"):

        regex = re.compile(r"^token\s*=\s*(\S.*)$")
        filename = os.path.expanduser(propsfile)
        self.token = token
        if self.token is None and os.path.isfile(filename):
            with open(filename, 'r') as props:
                self.token = next(iter([regex.match(line.strip()).group(1) for line in props if regex.match(line)]),
                                  None)
        if self.token is None:
            raise RuntimeError((
                'you must either provide an API token to this constructor, or create a '
                '.data.world file in your home directory with your API token'))

        self.protocol = protocol
        self.query_host = query_host
        self.apiHost = api_host

        self._api_client = ApiClient(host="{}://{}/v0".format(protocol, api_host), header_name='Authorization',
                                     header_value='Bearer {}'.format(token))
        self._datasets_api = DatasetsApi(self._api_client)
        self._uploads_api = UploadsApi(self._api_client)

    # Dataset Operations

    def get_dataset(self, dataset_key=None):
        owner_id, dataset_id = dataset_key.split('/')
        return self._datasets_api.get_dataset(owner_id, dataset_id)

    def create_dataset(self, owner_id=None, dataset=None):
        return self._datasets_api.create_dataset(owner_id, dataset)

    def patch_dataset(self, dataset_key=None, dataset=None):
        owner_id, dataset_id = dataset_key.split('/')
        return self._datasets_api.patch_dataset(owner_id, dataset_id, dataset)

    def replace_dataset(self, dataset_key=None, dataset=None):
        owner_id, dataset_id = dataset_key.split('/')
        return self._datasets_api.replace_dataset(owner_id, dataset_id, dataset)

    # File Operations

    def add_files_via_url(self, dataset_key=None, files=None):
        owner_id, dataset_id = dataset_key.split('/')
        return self._datasets_api.add_files_by_source(owner_id, dataset_id, files)

    def sync_files(self, dataset_key=None):
        owner_id, dataset_id = dataset_key.split('/')
        return self._datasets_api.sync(owner_id, dataset_id)

    def upload_files(self, dataset_key=None, files=None):
        owner_id, dataset_id = dataset_key.split('/')
        return self._uploads_api.upload_files(owner_id, dataset_id, files)

    def delete_files(self, dataset_key=None, names=None):
        owner_id, dataset_id = dataset_key.split('/')
        return self._datasets_api.delete_files_and_sync_sources(owner_id, dataset_id, names)

    # Query Operations

    def query(self, dataset, query, query_type="sql"):
        from . import __version__
        params = {
            "query": query
        }
        url = "{0}://{1}/{2}/{3}".format(self.protocol,
                                         self.query_host,
                                         query_type,
                                         dataset)
        headers = {
            'User-Agent': 'data.world-py - {0}'.format(__version__),
            'Accept': 'text/csv',
            'Authorization': 'Bearer {0}'.format(self.token)
        }
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            return Results(response.text)
        raise RuntimeError('error running query.')
