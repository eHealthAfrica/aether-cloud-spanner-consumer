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

from time import sleep

from app.helpers import (
    BQSchema,
    ClientException,
)

from . import *  # get all test assets from test/__init__.py

# Test Suite contains both unit and integration tests
# Unit tests can be run on their own from the root directory
# enter the bash environment for the version of python you want to test
# for example for python 3
# `docker-compose run consumer-sdk-test bash`
# then start the unit tests with
# `pytest -m unit`
# to run integration tests / all tests run the test_all.sh script from the /tests directory.


@pytest.mark.integration
def test__bq_resource(bq_resource):
    assert(bq_resource.test_connection() == True)


@pytest.mark.integration
def test__bq_mutate_schema_and_submit(
    any_sample_generator,
    bq_client,
    bq_table_generator,
    ANNOTATED_SCHEMA_V1,
    ANNOTATED_SCHEMA_V2  # speed this up a bit by only doing one transition
):
    
    fqn = bq_table_generator(ANNOTATED_SCHEMA_V1)
    project_id, dataset_id, table_id = fqn.split('.')

    i = 1
    for avro_schema in (
        ANNOTATED_SCHEMA_V1,
        ANNOTATED_SCHEMA_V2,
    ):
        LOG.error(f'migrating -> v{i}')
        table = bq_client.migrate_schema(table_id, avro_schema)
        samples = list(any_sample_generator(avro_schema, max=20, chunk=10))
        retry = 30
        start = 0
        wait = 10
        total = 0
        while retry:
            try:
                for x, chunk in enumerate(samples[start:]):  # don't retry successful batches
                    bq_client.write_rows(table_id, chunk)
                    start += 1
                break
            except ClientException as ce:
                if str(ce) != 'schema_mismatch':
                    raise ce
                retry -= 1
                LOG.debug(f'schema mismatch, waiting {wait}, previous total wait: {wait * (30 - retry)}')
                sleep(wait)


@pytest.mark.integration
def test__schema_transition(extend_kafka_topic, ANNOTATED_SCHEMA_V2, LoadedLocalJob):
    '''
    We're testing whether the job will be able to pause and wait for the BQ schema to be updated
    server side, while continuously resetting the offset to the change-over point
    '''
    extend_kafka_topic(ANNOTATED_SCHEMA_V2, 100)
    job = LoadedLocalJob
    ct = 0
    last_offset = None
    while max(job.get_current_offset().values() or [0]) <= 19:
        if ct >= 150:
            break
        job._run()
        offset = job.get_current_offset().values()
        if offset == last_offset:
            sleep(3)
        last_offset = offset
        ct += 1
    if ct >= 150:
        assert(False), 'test timed out'
    assert(True)


@pytest.mark.integration
def test__job_run(extend_kafka_topic, ANNOTATED_SCHEMA_V3, ANNOTATED_SCHEMA_V4, JobManagerRunningConsumer):
    extend_kafka_topic(ANNOTATED_SCHEMA_V3, 50)
    extend_kafka_topic(ANNOTATED_SCHEMA_V4, 50)
    job = JobManagerRunningConsumer.jobs.get(f'{TENANT}:default')
    assert(isinstance(job, artifacts.SpannerJob))
    for x in range(60):
        offset = max(LocalJob.get_offset_from_consumer(job.consumer).values() or [0])
        LOG.debug(offset)
        if offset >= 29:
            assert(True)
            return
        sleep(3)
    assert(False)
