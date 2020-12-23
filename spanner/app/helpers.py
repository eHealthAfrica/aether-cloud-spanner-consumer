#!/usr/bin/env python

# Copyright (C) 2020 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
from typing import Dict, Tuple

from google.auth.credentials import AnonymousCredentials
from google.oauth2 import service_account
from google.cloud.spanner import Client as SpannerClient
from google.cloud import bigquery


from google.api_core.exceptions import NotFound

from aet.logger import get_logger

# from app import config, utils


LOG = get_logger('HELPERS')


class MessageHandlingException(Exception):
    # A simple way to handle the variety of expected misbehaviors in message sync
    # Between Aether and Spanner
    pass


class Spanner(SpannerClient):

    def __init__(
        self,
        project: str = None,
        credentials: str = None,
        emulator_url: str = None
    ):

        if emulator_url:
            anon = AnonymousCredentials()
            super().__init__(
                project='local',
                credentials=anon,
                client_options={'api_endpoint': emulator_url}
            )
        elif project and credentials:
            creds = json.loads(credentials)
            creds = service_account.Credentials.from_service_account_info(credentials)
            super().__init__(project=project, credentials=creds)
        else:
            raise RuntimeError('invalid Spanner Client configuration')

    def _create_database(self, database):
        pass

    def _create_table(self, table):
        pass

    def check_writable(self, instance, database, table):
        pass


class BigQuery(bigquery.Client):

    def __init__(
        self,
        credentials: str = None
    ):

        credentials = json.loads(credentials)
        credentials = service_account.Credentials.from_service_account_info(
            credentials,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.project = credentials.project_id
        super().__init__(project=self.project, credentials=credentials)

    def _create_dataset(self, dataset):
        dataset_id = f'{self.project}.{dataset}'
        try:
            return self.get_dataset(dataset_id)
        except NotFound:
            raise RuntimeError(f'Target dataset {dataset_id} must exist')

    def _create_table(self, dataset, table, schema):

        table_id = f'{self.project}.{dataset}.{table}'
        LOG.debug(table_id)
        try:
            return self.get_table(table_id)
        except NotFound:
            pass
        table_ = bigquery.Table(table_id, schema=schema)
        self.create_table(table_)
        return table

    def write_rows(self, dataset, table, rows):
        # table_id = f'{self.project}.{dataset}.{table}'
        table_id = f'{self.project}.{dataset}.{table}'
        errors = self.insert_rows_json(table_id, rows)
        if not errors:
            return True
        raise MessageHandlingException(f'Insert Error: {errors}')


class BQSchema:

    AVRO_BASE = {
        "record": "RECORD",
        "string": "STRING",
        "boolean": "BOOLEAN",
        "bytes": "BYTES",
        "int": "INTEGER",
        "double": "FLOAT",
        "float": "FLOAT",
        "long": "INTEGER"
    }

    AET = {

    }

    @classmethod
    def __resolve_bq_type(cls, type_: str) -> str:
        try:
            return cls.AVRO_BASE[type_]
        except Exception as err:
            LOG.error(type_)
            raise err

    @classmethod
    def __mode_and_type(cls, field: Dict) -> Tuple[str, str]:  # Tuple[mode(nullable), BQ type]
        mode, type_ = cls.__primary_avro_type(field)
        bq_type = cls.__resolve_bq_type(type_)
        return (mode, bq_type)

    @classmethod
    def __primary_avro_type(cls, field: Dict) -> Tuple[str, str]:  # Tuple[nullable, avro type]
        type_ = field.get('type')
        if not isinstance(type_, list):
            return ('REQUIRED', type_)
        elif len(type_) == 0:
            return ('REQUIRED', type_)
        elif type_[0] == 'null':
            nested = [i for i in type_ if isinstance(i, dict)]
            if nested:
                n_type = nested[0].get('type')
                if n_type == 'record':
                    return ('NULLABLE', 'record')
                else:
                    return ('REPEATED', nested[0].get('items'))
            return ('REQUIRED', type_[1])
        nested = [i for i in type_ if isinstance(i, dict)]
        if nested:
            n_type = nested[0].get('type')
            if n_type == 'record':
                return ('NULLABLE', 'record')
            else:
                return ('REPEATED', nested[0].get('items'))
        return ('REQUIRED', type_[0])

    @classmethod
    def from_avro(cls, schema_: Dict):
        fields = schema_.get('fields')
        entries = [cls.xf_field(f) for f in fields]
        for e in entries:
            assert(isinstance(e.mode, str)), e.name
            assert(isinstance(e.field_type, str)), e.name
        return [i for i in entries if i]

    @classmethod
    def xf_field(cls, field: Dict):
        name = field.get('name')
        type_ = field.get('type')
        mode_, bg_type_ = cls.__mode_and_type(field)
        if bg_type_ == 'RECORD':
            fields = field.get('fields')
            if fields:
                return bigquery.SchemaField(
                    name,
                    "RECORD",
                    mode=mode_,
                    fields=cls.from_avro(field.get('fields')))
            nested_type = [i for i in type_ if isinstance(i, dict)]
            if nested_type:
                return bigquery.SchemaField(
                    name,
                    "RECORD",
                    mode=mode_,
                    fields=cls.from_avro(nested_type[0])
                )

        return bigquery.SchemaField(
            name,
            bg_type_,
            mode=mode_
        )
