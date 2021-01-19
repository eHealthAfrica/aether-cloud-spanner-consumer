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


import copy
import json
import pytest
import os
from time import sleep
from uuid import uuid4
from unittest.mock import patch

from redis import Redis
import requests

from google.api_core.exceptions import Unknown as UnknownError
from google.auth.credentials import AnonymousCredentials
from spavro.schema import parse

from aet.kafka_utils import (
    create_topic,
    delete_topic,
    get_producer,
    get_admin_client,
    produce
)

from aet.exceptions import MessageHandlingException
from aet.helpers import chunk_iterable
from aet.logger import callback_logger, get_logger
from aet.resource import InstanceManager

from aether.python.avro import generation

from app import artifacts, config, consumer, helpers
from app.fixtures import examples


CONSUMER_CONFIG = config.consumer_config
KAFKA_CONFIG = config.kafka_config


LOG = get_logger('FIXTURE')


URL = 'http://localhost:9013'
kafka_server = "kafka-test:29099"


# pick a random tenant for each run so we don't need to wipe ES.
TS = str(uuid4()).replace('-', '')[:8]
TENANT = f'TEN{TS}'
TEST_TOPIC = 'spanner_test_topic'
TEST_TABLE = f'test_table_{TS}'
DEFAULT_SPANNER_INSTANCE = 'bq_consumer_travis'
DEFAULT_BQDATASET = 'bq_consumer_travis'

GENERATED_SAMPLES = {}
# We don't want to initiate this more than once...


def check_spanner_alive(client):
    for _ in client.list_instance_configs():
        return
    else:
        raise RuntimeError('Spanner emulator not found')


class LocalJob(artifacts.SpannerJob):
    '''
        A way to allows tests to manually handle the run method without it automatically consuming
    '''

    @classmethod
    def get_offset_from_consumer(cls, consumer):
        # utility function used by tests
        partitions = consumer.assignment()
        partitions = consumer.position(partitions)
        return {p.topic: p.offset for p in partitions}


    def __init__(self, _id: str, tenant: str, config: dict, resources: InstanceManager = None):
        self._id = _id
        self.tenant = tenant
        self.resources = resources
        self.context = None
        self.config = config
        self._setup()
        # self._start()

    def _run(self):
        '''
        greatly simplified run loop, handles one message on call so we can step through
        '''

        config = copy.deepcopy(self.config)
        try:
            messages = self._get_messages(config)
            if messages:
                self._handle_messages(config, messages)
        except MessageHandlingException as mhe:
            self._on_message_handle_exception(mhe)
        except RuntimeError as rer:
            self.log.critical(f'RuntimeError: {self._id} | {rer}')
            self.safe_sleep(self.sleep_delay)

    def get_current_offset(self) -> dict:
        # utility function used by tests
        return self.get_offset_from_consumer(self.consumer)


@pytest.mark.integration
@pytest.fixture(scope='function')
def LoadedLocalJob(
    subscription_resource_definition,
    bq_instance_service_definition
):
    '''
    Expose a LocalJob wrapped Job
    '''
    im = InstanceManager(artifacts.SpannerJob._resources)
    for type_, ref in [
        ('subscription', subscription_resource_definition),
        ('bigquery', bq_instance_service_definition)
    ]:
        im.update(ref['id'], type_, TENANT, ref)
    conf = examples.JOB
    
    job = LocalJob(
        conf['id'],
        TENANT,
        conf,
        im
    )
    yield job
    if job.consumer:
        job.consumer.close()

@pytest.mark.integration
@pytest.fixture(scope='session')
def JobManagerRunningConsumer(
    subscription_resource_definition,
    bq_instance_service_definition,
    redis_client
):
    '''
    Expose the running job from a real consumer instance
    '''
    c = consumer.SpannerConsumer(CONSUMER_CONFIG, KAFKA_CONFIG, redis_client)
    job_manager = c.job_manager
    job = examples.JOB
    for type_, ref in [
        ('subscription', subscription_resource_definition),
        ('bigquery', bq_instance_service_definition)
    ]:
        job_manager.resources.update(ref['id'], type_, TENANT, ref)
    job_manager._init_job(job, TENANT)
    yield job_manager
    c.stop()



@pytest.mark.unit
@pytest.mark.integration
@pytest.fixture(scope='session')
def service_account_dict():
    account = os.environ.get('SERVICE_ACCOUNT')
    if account:
        yield json.loads(account.strip())
    else:
        yield None


@pytest.mark.unit
@pytest.mark.integration
@pytest.fixture(scope='session')
def spanner():
    spanner_ = helpers.Spanner(emulator_url='emu:9010')
    yield spanner_


@pytest.mark.unit
@pytest.mark.integration
@pytest.fixture(scope='session')
def bq_client(service_account_dict, sample_generator):
    client = helpers.BigQuery(
        credentials=json.dumps(service_account_dict),
        dataset=DEFAULT_BQDATASET
    )
    sa = client.get_service_account_email()
    assert sa is not None
    yield client
    client.close()


@pytest.mark.unit
@pytest.mark.integration
@pytest.fixture(scope='session')
def bq_table_generator(bq_client):
    client = bq_client
    dataset = DEFAULT_BQDATASET
    tables = [f'{client.project}.{dataset}.{TEST_TABLE}']  # created by examples.SUBSCRIPTION

    def fn(avro_schema):
        seed = str(uuid4()).replace('-', '')[:8]
        table = f'test_table_v{seed}'
        fqn = f'{client.project}.{dataset}.{table}'
        bq_schema = helpers.BQSchema.from_avro(avro_schema)
        client._create_table(table, schema=bq_schema)
        tables.append(fqn)
        return fqn

    yield fn
    for t in tables:
        try:
            res = client.delete_table(t)
            LOG.debug(f'removed table {t}: {res}')
        except Exception as err:
            LOG.error(f'could not remove table {t} from bq : {err}')


@pytest.mark.unit
@pytest.mark.integration
@pytest.fixture(scope='session')
def local_spanner_instance(spanner, service_account_dict):
    config = [i for i in spanner.list_instance_configs()][0]
    instance = spanner.instance(
        DEFAULT_SPANNER_INSTANCE,
        configuration_name=config.name,
        node_count=1,
        display_name='Test Instance')
    op = instance.create()
    _ = op.result()
    yield instance
    instance.delete()


@pytest.mark.unit
@pytest.mark.integration
@pytest.fixture(scope='session')
def RequestClientT1():
    s = requests.Session()
    s.headers.update({'x-oauth-realm': TENANT})
    yield s


@pytest.mark.unit
@pytest.fixture(scope='session')
def RequestClientT2():
    s = requests.Session()
    s.headers.update({'x-oauth-realm': f'{TENANT}-2'})
    yield s


@pytest.mark.unit
@pytest.fixture(scope='session')
def redis_client():
    password = os.environ.get('REDIS_PASSWORD')
    r = Redis(host='redis', password=password)
    yield r


@pytest.mark.unit
@pytest.fixture(scope='session')
def subscription_resource_definition():
    doc = copy.copy(examples.SUBSCRIPTION)
    doc['table'] = TEST_TABLE
    doc['topic-pattern'] = TEST_TOPIC
    yield doc

@pytest.fixture(scope='session')
def bq_instance_service_definition(service_account_dict):
    doc = copy.copy(examples.BQ_INSTANCE)
    doc['credential'] = service_account_dict
    doc['dataset'] = DEFAULT_BQDATASET
    yield doc


@pytest.fixture(scope='session')
def bq_resource(bq_instance_service_definition):
    inst = artifacts.BQInstance(
        TENANT,
        bq_instance_service_definition,
        None
    )
    assert(inst.test_connection() is True)
    yield inst
    inst.close()


@pytest.fixture(scope='session')
def ANNOTATED_SCHEMA_V1():
    yield ANNOTATED_SCHEMA


@pytest.fixture(scope='session')
def ANNOTATED_SCHEMA_V2(ANNOTATED_SCHEMA_V1):
    schema = copy.deepcopy(ANNOTATED_SCHEMA_V1)
    schema['fields'].append({
        'doc': 'A mandatory field, added later!',
        'name': 'extra_field',
        'type': 'string',
        'namespace': 'MySurvey.extra_field'
    })
    yield schema


@pytest.fixture(scope='session')
def ANNOTATED_SCHEMA_V3(ANNOTATED_SCHEMA_V2):
    schema = copy.deepcopy(ANNOTATED_SCHEMA_V2)
    # relax 'MySurvey.extra_field' -> nullable
    schema['fields'][-1]['type'] = [
        'null', 'string'
    ]
    geo_index = [x for x, field in enumerate(schema['fields']) if field['name'] == 'geometry'][0]
    # add a nested field to the geometry sub-record
    schema['fields'][geo_index]['type'][1]['fields'].append(
        {
            'doc': 'WhatThreeWords geocode',
            'name': 'threewords',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey.threewords'
        },
    )
    yield schema

@pytest.fixture(scope='session')
def ANNOTATED_SCHEMA_V4(ANNOTATED_SCHEMA_V3):
    schema = copy.deepcopy(ANNOTATED_SCHEMA_V3)
    # relax 'MySurvey.extra_field' -> required (illegal)
    schema['fields'][-1]['type'] = 'string'
    yield schema

# @pytest.mark.integration
@pytest.fixture(scope='session', autouse=True)
def create_remote_kafka_assets(request, sample_generator, *args):
    # @mark annotation does not work with autouse=True.
    if 'integration' not in request.config.invocation_params.args:
        LOG.debug(f'NOT creating Kafka Assets')
        yield None
    else:
        LOG.debug(f'Creating Kafka Assets')
        kafka_security = config.get_kafka_admin_config()
        kadmin = get_admin_client(kafka_security)
        new_topic = f'{TENANT}.{TEST_TOPIC}'
        create_topic(kadmin, new_topic)
        GENERATED_SAMPLES[new_topic] = []
        producer = get_producer(kafka_security)
        schema = parse(json.dumps(ANNOTATED_SCHEMA))
        for subset in sample_generator(max=100, chunk=10):
            GENERATED_SAMPLES[new_topic].extend(subset)
            res = produce(subset, schema, new_topic, producer)
            LOG.debug(res)
        yield None  # end of work before clean-up
        LOG.debug(f'deleting topic: {new_topic}')
        delete_topic(kadmin, new_topic)


@pytest.fixture(scope='session', autouse=True)
def extend_kafka_topic(any_sample_generator):
    kafka_security = config.get_kafka_admin_config()
    producer = get_producer(kafka_security)
    topic = f'{TENANT}.{TEST_TOPIC}'

    def fn(schema_dict, docs=10):
        schema_avro = parse(json.dumps(schema_dict))
        docs = any_sample_generator(schema_dict, max=docs, chunk=min([docs, 10]))
        for subset in docs:
            res = produce(subset, schema_avro, topic, producer)
            LOG.debug(res)
        producer.flush()
    
    yield fn


@pytest.fixture(scope='session', autouse=True)
def check_local_spanner_readyness(request, spanner, bq_client, *args):
    # @mark annotation does not work with autouse=True
    # if 'integration' not in request.config.invocation_params.args:
    #     LOG.debug(f'NOT Checking for LocalFirebase')
    #     return
    LOG.debug('Waiting for Spanner Emulator')
    for x in range(10):
        try:
            check_spanner_alive(spanner)
            LOG.debug(f'Spanner ready after {x} seconds')
            return
        except RuntimeError:
            sleep(1)

    raise TimeoutError('Could not connect to Spanner Emulator for tests')


@pytest.fixture(scope='session', autouse=True)
def make_local_spanner(request, local_spanner_instance, *args):
    LOG.debug(local_spanner_instance.display_name)



@pytest.mark.unit
@pytest.mark.integration
@pytest.fixture(scope='session')
def any_sample_generator():
    
    def _gen(schema, max=None, chunk=None):
        
        t = generation.SampleGenerator(schema)

        def _single(max):
            if not max:
                while True:
                    yield t.make_sample()
            for x in range(max):
                yield t.make_sample()

        def _chunked(max, chunk):
            return chunk_iterable(_single(max), chunk)

        if chunk:
            yield from _chunked(max, chunk)
        else:
            yield from _single(max)
    yield _gen



@pytest.mark.unit
@pytest.mark.integration
@pytest.fixture(scope='session')
def sample_generator():
    t = generation.SampleGenerator(ANNOTATED_SCHEMA)
    t.set_overrides('geometry.latitude', {'min': 44.754512, 'max': 53.048971})
    t.set_overrides('geometry.longitude', {'min': 8.013135, 'max': 28.456375})
    t.set_overrides('url', {'constant': 'http://ehealthafrica.org'})
    for field in ['beds', 'staff_doctors', 'staff_nurses']:
        t.set_overrides(field, {'min': 0, 'max': 50})

    def _gen(max=None, chunk=None):

        def _single(max):
            if not max:
                while True:
                    yield t.make_sample()
            for x in range(max):
                yield t.make_sample()

        def _chunked(max, chunk):
            return chunk_iterable(_single(max), chunk)

        if chunk:
            yield from _chunked(max, chunk)
        else:
            yield from _single(max)
    yield _gen


ANNOTATED_SCHEMA = {
    'doc': 'MySurvey (title: HS OSM Gather Test id: gth_hs_test, version: 2)',
    'name': 'MySurvey',
    'type': 'record',
    'fields': [
        {
            'doc': 'xForm ID',
            'name': '_id',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey'
        },
        {
            'doc': 'xForm version',
            'name': '_version',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_default_visualization': 'undefined'
        },
        {
            'doc': 'Surveyor',
            'name': '_surveyor',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey'
        },
        {
            'doc': 'Submitted at',
            'name': '_submitted_at',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'dateTime'
        },
        {
            'name': '_start',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'dateTime'
        },
        {
            'name': 'timestamp',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'dateTime'
        },
        {
            'name': 'username',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'name': 'source',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'name': 'osm_id',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Name of Facility',
            'name': 'name',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Address',
            'name': 'addr_full',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Phone Number',
            'name': 'contact_number',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Facility Operator Name',
            'name': 'operator',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Operator Type',
            'name': 'operator_type',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_default_visualization': 'pie',
            '@aether_lookup': [
                {
                    'label': 'Public',
                    'value': 'public'
                },
                {
                    'label': 'Private',
                    'value': 'private'
                },
                {
                    'label': 'Community',
                    'value': 'community'
                },
                {
                    'label': 'Religious',
                    'value': 'religious'
                },
                {
                    'label': 'Government',
                    'value': 'government'
                },
                {
                    'label': 'NGO',
                    'value': 'ngo'
                },
                {
                    'label': 'Combination',
                    'value': 'combination'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'Facility Location',
            'name': 'geometry',
            'type': [
                'null',
                {
                    'doc': 'Facility Location',
                    'name': 'geometry',
                    'type': 'record',
                    'fields': [
                        {
                            'doc': 'latitude',
                            'name': 'latitude',
                            'type': [
                                'null',
                                'float'
                            ],
                            'namespace': 'MySurvey.geometry'
                        },
                        {
                            'doc': 'longitude',
                            'name': 'longitude',
                            'type': [
                                'null',
                                'float'
                            ],
                            'namespace': 'MySurvey.geometry'
                        },
                        {
                            'doc': 'altitude',
                            'name': 'altitude',
                            'type': [
                                'null',
                                'float'
                            ],
                            'namespace': 'MySurvey.geometry'
                        },
                        {
                            'doc': 'accuracy',
                            'name': 'accuracy',
                            'type': [
                                'null',
                                'float'
                            ],
                            'namespace': 'MySurvey.geometry'
                        }
                    ],
                    'namespace': 'MySurvey',
                    '@aether_extended_type': 'geopoint'
                }
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'geopoint'
        },
        {
            'doc': 'Operational Status',
            'name': 'operational_status',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_default_visualization': 'pie',
            '@aether_lookup': [
                {
                    'label': 'Operational',
                    'value': 'operational'
                },
                {
                    'label': 'Non Operational',
                    'value': 'non_operational'
                },
                {
                    'label': 'Unknown',
                    'value': 'unknown'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'When is the facility open?',
            'name': '_opening_hours_type',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Pick the days of the week open and enter hours for each day',
                    'value': 'oh_select'
                },
                {
                    'label': 'Only open on weekdays with the same hours every day.',
                    'value': 'oh_weekday'
                },
                {
                    'label': '24/7 - All day, every day',
                    'value': 'oh_24_7'
                },
                {
                    'label': 'Type in OSM String by hand (Advanced Option)',
                    'value': 'oh_advanced'
                },
                {
                    'label': 'I do not know the operating hours',
                    'value': 'oh_unknown'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'Which days is this facility open?',
            'name': '_open_days',
            'type': [
                'null',
                {
                    'type': 'array',
                    'items': 'string'
                }
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Monday',
                    'value': 'Mo'
                },
                {
                    'label': 'Tuesday',
                    'value': 'Tu'
                },
                {
                    'label': 'Wednesday',
                    'value': 'We'
                },
                {
                    'label': 'Thursday',
                    'value': 'Th'
                },
                {
                    'label': 'Friday',
                    'value': 'Fr'
                },
                {
                    'label': 'Saturday',
                    'value': 'Sa'
                },
                {
                    'label': 'Sunday',
                    'value': 'Su'
                },
                {
                    'label': 'Public Holidays',
                    'value': 'PH'
                }
            ],
            '@aether_extended_type': 'select'
        },
        {
            'doc': 'Open hours by day of the week',
            'name': '_dow_group',
            'type': [
                'null',
                {
                    'doc': 'Open hours by day of the week',
                    'name': '_dow_group',
                    'type': 'record',
                    'fields': [
                        {
                            'doc': 'Enter open hours for each day:',
                            'name': '_hours_note',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Monday open hours',
                            'name': '_mon_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Tuesday open hours',
                            'name': '_tue_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Wednesday open hours',
                            'name': '_wed_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Thursday open hours',
                            'name': '_thu_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Friday open hours',
                            'name': '_fri_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Saturday open hours',
                            'name': '_sat_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Sunday open hours',
                            'name': '_sun_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Public Holiday open hours',
                            'name': '_ph_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'name': '_select_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey._dow_group',
                            '@aether_extended_type': 'string'
                        }
                    ],
                    'namespace': 'MySurvey',
                    '@aether_extended_type': 'group'
                }
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'group'
        },
        {
            'doc': 'Enter weekday hours',
            'name': '_weekday_hours',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'OSM:opening_hours',
            'name': '_advanced_hours',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'name': 'opening_hours',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Verify the open hours are correct or go back and fix:',
            'name': '_disp_hours',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Facility Category',
            'name': 'amenity',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Clinic',
                    'value': 'clinic'
                },
                {
                    'label': 'Doctors',
                    'value': 'doctors'
                },
                {
                    'label': 'Hospital',
                    'value': 'hospital'
                },
                {
                    'label': 'Dentist',
                    'value': 'dentist'
                },
                {
                    'label': 'Pharmacy',
                    'value': 'pharmacy'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'Available Services',
            'name': 'healthcare',
            'type': [
                'null',
                {
                    'type': 'array',
                    'items': 'string'
                }
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Doctor',
                    'value': 'doctor'
                },
                {
                    'label': 'Pharmacy',
                    'value': 'pharmacy'
                },
                {
                    'label': 'Hospital',
                    'value': 'hospital'
                },
                {
                    'label': 'Clinic',
                    'value': 'clinic'
                },
                {
                    'label': 'Dentist',
                    'value': 'dentist'
                },
                {
                    'label': 'Physiotherapist',
                    'value': 'physiotherapist'
                },
                {
                    'label': 'Alternative',
                    'value': 'alternative'
                },
                {
                    'label': 'Laboratory',
                    'value': 'laboratory'
                },
                {
                    'label': 'Optometrist',
                    'value': 'optometrist'
                },
                {
                    'label': 'Rehabilitation',
                    'value': 'rehabilitation'
                },
                {
                    'label': 'Blood donation',
                    'value': 'blood_donation'
                },
                {
                    'label': 'Birthing center',
                    'value': 'birthing_center'
                }
            ],
            '@aether_extended_type': 'select'
        },
        {
            'doc': 'Specialities',
            'name': 'speciality',
            'type': [
                'null',
                {
                    'type': 'array',
                    'items': 'string'
                }
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'xx',
                    'value': 'xx'
                }
            ],
            '@aether_extended_type': 'select'
        },
        {
            'doc': 'Speciality medical equipment available',
            'name': 'health_amenity_type',
            'type': [
                'null',
                {
                    'type': 'array',
                    'items': 'string'
                }
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Ultrasound',
                    'value': 'ultrasound'
                },
                {
                    'label': 'MRI',
                    'value': 'mri'
                },
                {
                    'label': 'X-Ray',
                    'value': 'x_ray'
                },
                {
                    'label': 'Dialysis',
                    'value': 'dialysis'
                },
                {
                    'label': 'Operating Theater',
                    'value': 'operating_theater'
                },
                {
                    'label': 'Laboratory',
                    'value': 'laboratory'
                },
                {
                    'label': 'Imaging Equipment',
                    'value': 'imaging_equipment'
                },
                {
                    'label': 'Intensive Care Unit',
                    'value': 'intensive_care_unit'
                },
                {
                    'label': 'Emergency Department',
                    'value': 'emergency_department'
                }
            ],
            '@aether_extended_type': 'select'
        },
        {
            'doc': 'Does this facility provide Emergency Services?',
            'name': 'emergency',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Yes',
                    'value': 'yes'
                },
                {
                    'label': 'No',
                    'value': 'no'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'Does the pharmacy dispense prescription medication?',
            'name': 'dispensing',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Yes',
                    'value': 'yes'
                },
                {
                    'label': 'No',
                    'value': 'no'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'Number of Beds',
            'name': 'beds',
            'type': [
                'null',
                'int'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'int',
            '@aether_masking': 'private'
        },
        {
            'doc': 'Number of Doctors',
            'name': 'staff_doctors',
            'type': [
                'null',
                'int'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'int',
            '@aether_masking': 'private'
        },
        {
            'doc': 'Number of Nurses',
            'name': 'staff_nurses',
            'type': [
                'null',
                'int'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'int',
            '@aether_masking': 'private'
        },
        {
            'doc': 'Types of insurance accepted?',
            'name': 'insurance',
            'type': [
                'null',
                {
                    'type': 'array',
                    'items': 'string'
                }
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Public',
                    'value': 'public'
                },
                {
                    'label': 'Private',
                    'value': 'private'
                },
                {
                    'label': 'None',
                    'value': 'no'
                },
                {
                    'label': 'Unknown',
                    'value': 'unknown'
                }
            ],
            '@aether_extended_type': 'select',
            '@aether_masking': 'public'
        },
        {
            'doc': 'Is this facility wheelchair accessible?',
            'name': 'wheelchair',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Yes',
                    'value': 'yes'
                },
                {
                    'label': 'No',
                    'value': 'no'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'What is the source of water for this facility?',
            'name': 'water_source',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Well',
                    'value': 'well'
                },
                {
                    'label': 'Water works',
                    'value': 'water_works'
                },
                {
                    'label': 'Manual pump',
                    'value': 'manual_pump'
                },
                {
                    'label': 'Powered pump',
                    'value': 'powered_pump'
                },
                {
                    'label': 'Groundwater',
                    'value': 'groundwater'
                },
                {
                    'label': 'Rain',
                    'value': 'rain'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'What is the source of power for this facility?',
            'name': 'electricity',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_lookup': [
                {
                    'label': 'Power grid',
                    'value': 'grid'
                },
                {
                    'label': 'Generator',
                    'value': 'generator'
                },
                {
                    'label': 'Solar',
                    'value': 'solar'
                },
                {
                    'label': 'Other Power',
                    'value': 'other'
                },
                {
                    'label': 'No Power',
                    'value': 'none'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'URL for this location (if available)',
            'name': 'url',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'In which health are is the facility located?',
            'name': 'is_in_health_area',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'In which health zone is the facility located?',
            'name': 'is_in_health_zone',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'string'
        },
        {
            'name': 'meta',
            'type': [
                'null',
                {
                    'name': 'meta',
                    'type': 'record',
                    'fields': [
                        {
                            'name': 'instanceID',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'MySurvey.meta',
                            '@aether_extended_type': 'string'
                        }
                    ],
                    'namespace': 'MySurvey',
                    '@aether_extended_type': 'group'
                }
            ],
            'namespace': 'MySurvey',
            '@aether_extended_type': 'group'
        },
        {
            'doc': 'UUID',
            'name': 'id',
            'type': 'string'
        }
    ],
    'namespace': 'org.ehealthafrica.aether.odk.xforms.Mysurvey'
}
