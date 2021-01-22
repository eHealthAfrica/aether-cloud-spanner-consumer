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

BQ_INSTANCE = {
    'id': 'default',
    'name': 'the default instance',
    'project': 'some-project',                  # Project ID of Spanner instance
    'credential': {'json': 'doc'},              # credentials document
    'dataset': 'test_ds'
}


SPANNER_INSTANCE = {
    'id': 'default',
    'name': 'the default instance',
    'project': 'some-project',                  # Project ID of Spanner instance
    'credential': {'json': 'doc'},              # credentials document
    'instance': 'test_instance',
    'database': 'test_db'
}


SUBSCRIPTION = {
    'id': 'sub-test',
    'name': 'Test Subscription',
    'table': 'test_table',                        # Output table to write to
    'topic_pattern': '*',
    'topic_options': {
        'masking_annotation': '@aether_masking',  # schema key for mask level of a field
        'masking_levels': ['public', 'private'],  # classifications
        'masking_emit_level': 'public',           # emit from this level ->
        'filter_required': False,                 # filter on a message value?
        'filter_field_path': 'operational_status',    # which field?
        'filter_pass_values': ['operational'],             # what are the passing values?
    }                                              # or hard-code like a/b/c
}

JOB = {
    'id': 'default',
    'name': 'Default Google Consumer Job',
    'bigquery': 'default',
    'subscription': ['sub-test']
}
