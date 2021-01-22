#!/usr/bin/env python

# Copyright (C) 2021 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
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

from collections import defaultdict
import json
from typing import (
    Any, Dict, List, Tuple
)

from google.api_core.exceptions import BadRequest, NotFound
from google.auth.credentials import AnonymousCredentials
from google.cloud.spanner import Client as SpannerClient
from google.cloud import bigquery
from google.oauth2 import service_account

from aet.exceptions import MessageHandlingException
from aet.logger import get_logger


LOG = get_logger('HELPERS')


class ClientException(MessageHandlingException):
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

    def migrate_schema(self, dataset_id, table_id, avro_schema):
        pass

    def check_writable(self, instance, database, table):
        pass

    def write_rows(self, table, rows):
        pass


class BigQuery(bigquery.Client):

    def __init__(
        self,
        credentials: str = None,
        dataset: str = None
    ):

        credentials = json.loads(credentials)
        credentials = service_account.Credentials.from_service_account_info(
            credentials,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.project = credentials.project_id
        self.dataset = dataset
        super().__init__(project=self.project, credentials=credentials)

    def _create_dataset(self):
        dataset_id = f'{self.project}.{self.dataset}'
        try:
            return self.get_dataset(dataset_id)
        except (NotFound, BadRequest):
            raise RuntimeError(f'Target dataset {dataset_id} must exist, create it in BQ admin interface')

    def _create_table(self, table, schema):

        table_id = f'{self.project}.{self.dataset}.{table}'
        LOG.debug(table_id)
        try:
            return self.get_table(table_id)
        except NotFound:
            pass
        except BadRequest as ber:
            raise ber
        table_ = bigquery.Table(table_id, schema=schema)
        self.create_table(table_)
        return table

    def migrate_schema(self, table_id, avro_schema):  # -> bigquery.Table
        fqn = f'{self.project}.{self.dataset}.{table_id}'
        try:
            table = self.get_table(fqn)
        except NotFound:
            LOG.info(f'table {table_id} does not exist, creating')
            return self._create_table(
                table_id,
                BQSchema.from_avro(avro_schema)
            )
        except BadRequest as ber:
            raise ber
        LOG.info(f'Migrating {table_id}.')
        original_schema = table.schema
        new_schema = BQSchema.merge_schemas(
            original_schema,
            BQSchema.from_avro(avro_schema))
        table.schema = new_schema
        self.update_table(table, ['schema'])
        # there is no way to check to see if the new schema has propagated
        # except to fail to submit data
        return table

    def write_rows(self, table, rows):
        table_id = f'{self.project}.{self.dataset}.{table}'
        errors = self.insert_rows_json(
            table_id,
            rows
        )
        self._handle_errors(errors)
        return True

    @classmethod
    def _handle_errors(cls, errors) -> None:  # raises ClientException
        HANDLED_ERROR_TYPES = [
            # ordered by importance to us
            # (REASON, MATCH_STRING, ALIAS)
            ('invalid', 'no such field', 'schema_mismatch')
        ]
        res = defaultdict(set)
        for blk in errors:
            for err in blk.get('errors'):
                res[err['reason']].add(f'{err["message"]} : {err["location"]}')
        for reason, match, alias in HANDLED_ERROR_TYPES:
            if reason in res and any([True for err in res[reason] if match in err]):
                raise ClientException(alias, details=res)
        if len(res.keys()) > 0:
            raise ClientException('unexpected BigQuery client error', details=res)


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
        "dateTime": "TIMESTAMP",
        # "geopoint": ""  # single geopoints are not castable to the GEOGRAPHY TYPE,
        # but the BQ type can be constructed by query from float lat/long using
        # ST_GEOGPOINT(longitude, latitude)
    }

    @classmethod
    def __resolve_bq_type(cls, avro_type: str, field: Dict) -> str:
        extended_type = field.get('@aether_extended_type')
        if extended_type and extended_type in cls.AET:
            return cls.AET[extended_type]
        try:
            return cls.AVRO_BASE[avro_type]
        except Exception as err:
            LOG.error(avro_type)
            raise err

    @classmethod
    def __mode_and_type(cls, field: Dict) -> Tuple[str, str]:  # Tuple[mode, BQ type]
        mode, avro_type = cls.__primary_avro_type(field)
        bq_type = cls.__resolve_bq_type(avro_type, field)
        return (mode, bq_type)

    @classmethod
    def __handle_nested_type(cls, fields: List) -> Tuple[str, str]:  # Tuple[mode, avro type]:
        # can't have multiple nested types in the same BQ column so we pick the more preferred
        type_ = fields[0].get('type')
        if type_ == 'record':
            return ('NULLABLE', 'record')
        else:
            return ('REPEATED', fields[0].get('items'))

    @classmethod
    def __primary_avro_type(cls, field: Dict) -> Tuple[str, str]:  # Tuple[mode, avro type]
        type_ = field.get('type')
        if not isinstance(type_, list):
            return ('REQUIRED', type_)
        if len(type_) == 0:
            return ('REQUIRED', type_)
        nested = [i for i in type_ if isinstance(i, dict)]
        if nested:
            return cls.__handle_nested_type(nested)
        if type_[0] == 'null':
            return ('NULLABLE', type_[1])
        return ('REQUIRED', type_[0])

    @classmethod
    def from_avro(cls, schema_: Dict):
        fields = schema_.get('fields')
        entries = [cls.__xf_field(f) for f in fields]
        return [i for i in entries if i]

    @classmethod
    def __xf_field(cls, field: Dict):
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

    @classmethod
    def detect_schema_changes(
        cls,
        old: List[bigquery.SchemaField],
        new: List[bigquery.SchemaField]
    ) -> Dict[str, List[bigquery.SchemaField]]:

        old_names = set([i.name for i in old])
        new_names = set([i.name for i in new])
        new_fields = select_by_names(new, list(new_names - old_names))
        overlap = old_names.intersection(new_names)
        updated_fields = [
            nf for nf in select_by_names(new, overlap)
            for of_ in select_by_names(old, overlap)
            if (nf.name == of_.name and nf != of_)
        ]
        return {
            'new': new_fields,
            'updated': updated_fields
        }

    @classmethod
    def merge_schemas(
        cls,
        old: List[bigquery.SchemaField],
        new: List[bigquery.SchemaField]
    ) -> List[bigquery.SchemaField]:
        # SchemaField.fields becomes a tuple after construction
        res = list(old)[:]  # must be additive to old schema
        diff = cls.detect_schema_changes(old, new)
        for f in diff.get('updated', []):
            old_field = select_by_name(res, f.name)
            if len(f.fields) > 0:
                # SchemaField.fields cannot be replaced so we make a new instance
                mode = (
                    f.mode if cls.mode_change_allowed(old_field.mode, f.mode)
                    else old_field.mode)
                replace_in_place(
                    res,
                    old_field,
                    bigquery.SchemaField(
                        f.name,
                        f.field_type,
                        mode,
                        fields=cls.merge_schemas(old_field.fields, f.fields)
                    )
                )
            else:
                if cls.field_changes_allowed(old_field, f):
                    replace_in_place(res, old_field, f)
        for f in diff.get('new', []):
            if not f.is_nullable:
                #  cannot add a mandatory field after table is created'
                corrected = bigquery.SchemaField(
                    f.name,
                    f.field_type,
                    'NULLABLE',
                    fields=f.fields or ()
                )
                LOG.info(f'NEW -> {corrected.name}, {corrected.field_type}, {corrected.mode}, {corrected.fields}')
                res.append(corrected)
            else:
                res.append(f)
        return res

    @classmethod
    def field_changes_allowed(
        cls,
        old: bigquery.SchemaField,
        new: bigquery.SchemaField
    ) -> bool:
        try:
            assert(cls.mode_change_allowed(old.mode, new.mode))
            assert(old.field_type == new.field_type)
            return True
        except AssertionError:
            return False

    @classmethod
    def mode_change_allowed(
        cls,
        old: str,  # mode
        new: str   # mode
    ) -> bool:

        if old == new:
            return True
        allowed = [
            # BQ only allows the relaxation of columns, not the opposite, or other changes
            ('REQUIRED', 'NULLABLE')
        ]
        if (old, new) in allowed:
            return True
        return False


def select_by_names(items: List, names: List[str]):
    return [i for i in items if i.name in names]


def select_by_name(items: List, name: str):
    return select_by_names(items, [name])[0]


def replace_in_place(parent: List, old_child: Any, new_child: Any):
    idx = parent.index(old_child)
    parent[idx] = new_child
