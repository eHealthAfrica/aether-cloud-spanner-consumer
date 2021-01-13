#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
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

from copy import copy
import operator
import pytest


from app.helpers import (
    BigQuery, BQSchema, ClientException, replace_in_place, select_by_name, select_by_names)

from . import *  # get all test assets from test/__init__.py
# from app.fixtures import examples
# from app.artifacts import Subscription

# Test Suite contains both unit and integration tests
# Unit tests can be run on their own from the root directory
# enter the bash environment for the version of python you want to test
# for example for python 3
# `docker-compose run consumer-sdk-test bash`
# then start the unit tests with
# `pytest -m unit`
# to run integration tests / all tests run the test_all.sh script from the /tests directory.


class An(object):
    def __init__(self, i):
        self.name = i


@pytest.mark.unit
def test__replace_in_place():
    list_ = [An(x) for x in range(5)]
    first = list_[0]
    last = list_[-1]
    replace_in_place(list_, last, An(6))
    replace_in_place(list_, first, An(-1))
    assert(first not in list_)
    assert(last not in list_)
    assert(list_[0].name == -1)
    assert(list_[-1].name == 6)


@pytest.mark.unit
def test__select_by_name():
    list_ = [An(x) for x in range(5)]
    zero = select_by_name(list_, 0)
    assert(zero is list_[0])
    more_than_zero = select_by_names(list_, [0, 1])
    assert(len(more_than_zero) > 1)
    assert(zero in more_than_zero)


@pytest.mark.parametrize('old,new,ok', [
    ('NULLABLE', 'NULLABLE', True),
    ('REQUIRED', 'NULLABLE', True),
    ('NULLABLE', 'REQUIRED', False)
])
@pytest.mark.unit
def test__mode_change(old, new, ok):
    assert(
        BQSchema.mode_change_allowed(old, new) is ok
    )


@pytest.mark.unit
def test__bq_generate_schema(ANNOTATED_SCHEMA_V1):
    schema = BQSchema.from_avro(ANNOTATED_SCHEMA_V1)
    assert(len(schema) == len(ANNOTATED_SCHEMA_V1['fields']))
    name_ = operator.itemgetter('name')
    geo = select_by_name(schema, 'geometry')
    geo_avro = list(filter(lambda x: name_(x) == 'geometry', ANNOTATED_SCHEMA_V1['fields']))[0]
    assert(len(geo.fields) == len(geo_avro['type'][1]['fields']))

@pytest.mark.unit
def test__bq_detect_schema_changes(ANNOTATED_SCHEMA_V1, ANNOTATED_SCHEMA_V2, ANNOTATED_SCHEMA_V3):
    one = BQSchema.from_avro(ANNOTATED_SCHEMA_V1)
    two = BQSchema.from_avro(ANNOTATED_SCHEMA_V2)
    three = BQSchema.from_avro(ANNOTATED_SCHEMA_V3)
    
    changes = BQSchema.detect_schema_changes(one, two)
    assert(
        select_by_names(changes['new'], ['extra_field']) is not [] and
        not changes.get('updated'))
    
    changes = BQSchema.detect_schema_changes(one, three)
    assert(
        select_by_names(changes['new'], ['extra_field']) is not [] and
        select_by_names(changes['updated'], ['geometry']) is not [])
    
    changes = BQSchema.detect_schema_changes(two, three)
    assert(
        select_by_names(changes['updated'], ['extra_field']) is not [] and 
        select_by_names(changes['updated'], ['geometry']) is not [])

@pytest.mark.unit
def test__bq_merge_schemas(ANNOTATED_SCHEMA_V1, ANNOTATED_SCHEMA_V2, ANNOTATED_SCHEMA_V3, ANNOTATED_SCHEMA_V4):
    one = BQSchema.from_avro(ANNOTATED_SCHEMA_V1)
    two = BQSchema.from_avro(ANNOTATED_SCHEMA_V2)
    three = BQSchema.from_avro(ANNOTATED_SCHEMA_V3)
    four = BQSchema.from_avro(ANNOTATED_SCHEMA_V4)
    
    one_three = BQSchema.merge_schemas(one, three)
    assert(
        len(select_by_name(one, 'geometry').fields) <
        len(select_by_name(one_three, 'geometry').fields))
    
    one_two = BQSchema.merge_schemas(one, two)
    assert(
        len(one_two) > len(one))

    three_four = BQSchema.merge_schemas(three, four)  # change is illegal (making optional field mandatory)
    # Field is mandatory in new schema
    assert(select_by_name(four, 'extra_field').is_nullable is False)
    # Field should still be mandatory after reconciliations with old
    assert(select_by_name(three_four, 'extra_field').is_nullable is True)

@pytest.mark.unit
def test__bq_handle_errors():
    err = [
        {
            'index': 0,
            'errors': [
                {
                    'reason': 'invalid',
                    'location': 'extra_field',
                    'debugInfo': '',
                    'message': 'no such field.'
                }
            ]
        },
        {
            'index': 1,
            'errors': [
                {
                    'reason': 'invalid',
                    'location': 'extra_field',
                    'debugInfo': '',
                    'message': 'no such field.'
                }
            ]
        },
        {
            'index': 2,
            'errors': [
                {
                    'reason': 'invalid',
                    'location': 'extra_field',
                    'debugInfo': '',
                    'message': 'no such field.'
                }
            ]
        }
    ]

    
    try:
        BigQuery._handle_errors(err)
    except ClientException as ce:
        assert(str(ce) == 'schema_mismatch')
        assert(len(ce.details.keys()) == 1)
        assert(len(ce.details['invalid']) == 1)


@pytest.mark.parametrize('name,ok', [
    ('NULLABLE', True),
    ('Mixed', True),
    ('not_camel_case', True),
    ('fewer_charaters', True),
    ('Ver-boten', False),
])
@pytest.mark.unit
def test__subscription_validation(name, ok):
    def_ = {'table': name}
    if not ok:
        with pytest.raises(AssertionError):
            artifacts.Subscription._secondary_validation(def_)
    else:
        assert(artifacts.Subscription._secondary_validation(def_) is None)
