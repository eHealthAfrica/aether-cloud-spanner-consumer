#!/usr/bin/env python

# Copyright (C) 2019 by eHealth Africa : http://www.eHealthAfrica.org
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

import fnmatch
import json
from time import sleep
from typing import (
    # Any,
    Callable,
    List,
    # Mapping
)

from confluent_kafka import KafkaException

# Spanner


# Consumer SDK
from aet.exceptions import ConsumerHttpException, MessageHandlingException
from aet.job import BaseJob, JobStatus
from aet.kafka import KafkaConsumer, FilterConfig, MaskConfig
from aet.logger import callback_logger, get_logger
from aet.resource import BaseResource, lock
from werkzeug.local import LocalProxy

# Aether python lib
# from aether.python.avro.schema import Node

from app.config import get_consumer_config, get_kafka_config
from app.fixtures import schemas

from app import helpers

LOG = get_logger('artifacts')
CONSUMER_CONFIG = get_consumer_config()
KAFKA_CONFIG = get_kafka_config()


class BQInstance(BaseResource):
    schema = schemas.BQ_INSTANCE
    jobs_path = '$.bigquery'
    name = 'bigquery'
    public_actions = BaseResource.public_actions + [
        'test_connection'
    ]

    def __init__(self, tenant, definition):
        super().__init__(tenant, definition)
        self.bq = None
        self.dataset = None

    def get_bq(self) -> helpers.BigQuery:
        if not self.bq:
            service_account_dict = json.loads(self.definition['credential'])
            self.bq = helpers.BigQuery(credentials=json.dumps(service_account_dict))
            self.dataset = self.definition['dataset']
        return self.bq

    def test_connection(self, *args, **kwargs):
        try:
            LOG.info('testing connection')
            client = self.get_bq()
            sa = client.get_service_account_email()
            if not sa:
                raise Exception('Could not fetch service account')
            client._create_dataset(self.dataset)
            return True
        except Exception as unexpected:
            raise ConsumerHttpException(unexpected, 500)

    def close(self):
        if self.bq:
            self.bq.close()
            self.bq = None


class SpannerInstance(BaseResource):
    schema = schemas.SPANNER_INSTANCE
    jobs_path = '$.spanner'
    name = 'spanner'
    public_actions = BaseResource.public_actions + [
        'test_connection'
    ]

    def __init__(self, tenant, definition, app=None):
        super().__init__(tenant, definition)
        # makes unit tests easier
        if app:
            self.app = app

    @lock
    def get_session(self):
        if self.app:
            return self.app
        # name = self.definition["name"]
        # credentials = firebase_credentials.Certificate(self.definition['credential'])
        # LOG.debug('created credentials')
        # options = {'databaseURL': self.definition['url']}
        # LOG.debug(f'making app with {name}, {credentials}, {options}')
        # self.app = firebase_admin.initialize_app(
        #     name=name,
        #     credential=credentials,
        #     options=options
        # )
        # LOG.info('App initialized')
        # return self.app

    def get_spanner(self):
        pass

    def test_connection(self, *args, **kwargs):
        # try:
        #     LOG.info('testing connection')
        #     rtdb = self.get_rtdb()
        #     LOG.info('got reference')
        #     ref = rtdb.reference('some/path')
        #     cref = self.get_cloud_firestore().collection(
        #         u'testconnection').document(u'consumerping')
        #     return (ref and cref) is not None
        # except UnavailableError as err:
        #     raise ConsumerHttpException(err, 500)
        # except Exception as unexpected:
        #     raise ConsumerHttpException(unexpected, 500)
        pass


class Subscription(BaseResource):
    schema = schemas.SUBSCRIPTION
    jobs_path = '$.subscription'
    name = 'subscription'

    @classmethod
    def _validate(cls, definition) -> bool:
        if not super(Subscription, cls)._validate(definition):
            return False
        try:
            cls._secondary_validation(definition)
        except AssertionError:
            return False
        return True

    @classmethod
    def _validate_pretty(cls, definition, *args, **kwargs):
        _res = super(Subscription, cls)._validate_pretty(definition, *args, **kwargs)
        try:
            if isinstance(definition, LocalProxy):
                definition = definition.get_json()
            cls._secondary_validation(definition)
        except AssertionError as aer:
            if _res['valid']:
                return {
                    'valid': False,
                    'validation_errors': [str(aer)]
                }
            else:
                _res['validation_errors'].append(str(aer))
        return _res

    @classmethod
    def _secondary_validation(cls, definition):
        # raises AssertionError on Failure
        target = definition.get(
            'fb_options', {}).get(
            'target_path')
        if target:
            assert(len(target.split('/')) % 2 != 0), f'target path"{target}" must be even'
            wc = '{topic}'
            if wc in target:
                no_wc = target.replace(wc, '')
            else:
                no_wc = target
            assert('{' not in no_wc), f'extra replacement strings in {target}'

    def _handles_topic(self, topic, tenant):
        topic_str = self.definition.topic_pattern
        # remove tenant information
        no_tenant = topic.lstrip(f'{tenant}.')
        return fnmatch.fnmatch(no_tenant, topic_str)

    def path_for_topic(self, topic):
        _path = self.definition.get(
            'fb_options', {}).get(
            'target_path', '_aether/entities/{topic}')
        if '{topic}' in _path:
            _path = _path.format(topic=topic)
        return _path


class SpannerJob(BaseJob):
    name = 'job'
    # Any type here needs to be registered in the API as APIServer._allowed_types
    _resources = [SpannerInstance, Subscription]
    schema = schemas.SPANNER_JOB

    public_actions = BaseJob.public_actions + [
        'list_assigned_topics',
        'list_subscribed_topics',
        'list_topics'
    ]
    # publicly available list of topics
    subscribed_topics: dict
    log_stack: list
    log: Callable  # each job instance has it's own log object to keep log_stacks -> user reportable

    consumer: KafkaConsumer = None
    # processing artifacts
    _indices: dict
    _schemas: dict
    _previous_topics: list
    _spanner: SpannerInstance
    _subscriptions: List[Subscription]

    def _setup(self):
        self.subscribed_topics = {}
        self._schemas = {}
        self._subscriptions = []
        self._previous_topics = []
        self.log_stack = []
        self.log = callback_logger('JOB', self.log_stack, 100)
        self.group_name = f'{self.tenant}.gspanner.{self._id}'
        self.sleep_delay: float = 0.5
        self.report_interval: int = 100
        args = {k.lower(): v for k, v in KAFKA_CONFIG.copy().items()}
        args['group.id'] = self.group_name
        LOG.debug(args)
        self.consumer = KafkaConsumer(**args)

    def _job_spanner(self, config=None) -> SpannerInstance:
        if config:
            spanner: List[SpannerInstance] = self.get_resources('spanner', config)
            if not spanner:
                raise ConsumerHttpException('No Spanner associated with Job', 400)
            self._spanner = spanner[0]
        return self._spanner

    def _job_subscriptions(self, config=None) -> List[Subscription]:
        if config:
            subs = self.get_resources('subscription', config)
            if not subs:
                raise ConsumerHttpException('No Subscriptions associated with Job', 400)
            self._subscriptions = subs
        return self._subscriptions

    def _job_subscription_for_topic(self, topic) -> Subscription:
        return next(iter(
            sorted([
                i for i in self._job_subscriptions()
                if i._handles_topic(topic, self.tenant)
            ])),
            None)

    def _test_connections(self, config):
        self._job_subscriptions(config)
        # self._job_firebase(config).test_connection()  # raises CHE
        return True

    def _get_messages(self, config):
        try:
            self.log.debug(f'{self._id} checking configurations...')
            self._test_connections(config)
            subs = self._job_subscriptions()
            self._handle_new_subscriptions(subs)
            self.log.debug(f'Job {self._id} getting messages')
            return self.consumer.poll_and_deserialize(
                timeout=5,
                num_messages=1)  # max
        except ConsumerHttpException as cer:
            # don't fetch messages if we can't post them
            self.log.debug(f'Job not ready: {cer}')
            self.status = JobStatus.RECONFIGURE
            sleep(self.sleep_delay * 10)
            return []
        except Exception as err:
            import traceback
            traceback_str = ''.join(traceback.format_tb(err.__traceback__))
            self.log.critical(f'unhandled error: {str(err)} | {traceback_str}')
            raise err
            sleep(self.sleep_delay)
            return []

    def _handle_new_subscriptions(self, subs):
        old_subs = list(sorted(set(self.subscribed_topics.values())))
        for sub in subs:
            pattern = sub.definition.topic_pattern
            # only allow regex on the end of patterns
            if pattern.endswith('*'):
                self.subscribed_topics[sub.id] = f'^{self.tenant}.{pattern}'
            else:
                self.subscribed_topics[sub.id] = f'{self.tenant}.{pattern}'
        new_subs = list(sorted(set(self.subscribed_topics.values())))
        _diff = list(set(old_subs).symmetric_difference(set(new_subs)))
        if _diff:
            self.log.info(f'{self.tenant} added subs to topics: {_diff}')
            self.consumer.subscribe(new_subs, on_assign=self._on_assign)

    # def _handle_messages(self, config, messages):
    #     self.log.debug(f'{self.group_name} | reading {len(messages)} messages')
    #     count = 0
    #     subs = {}
    #     # spanner: SpannerInstance = self._job_spanner()
    #     client = None
    #     first_offset = messages[0].offset
    #     last_offset = None
    #     try:
    #         batch = []
    #         for msg in messages:
    #             last_offset = msg.offset
    #             topic = msg.topic
    #             if topic not in subs:
    #                 subs[topic] = self._job_subscription_for_topic(topic)
    #             subscription = subs[topic]
    #             schema = msg.schema
    #             if schema != self._schemas.get(topic):
    #                 self.log.info(f'{self._id} Schema change on {topic}')
    #                 self._update_topic(client, subscription, topic, schema)
    #                 self._schemas[topic] = schema
    #             else:
    #                 self.log.debug('Schema unchanged.')
    #             batch.append(msg.value)
    #             # self.add_message(msg.value, topic, subs[topic], cfs, batch)
    #             count += 1
    #             if (count % MAX_SUBMIT) == 0:
    #                 # batch.commit()
    #                 # batch = cfs.batch()
    #                 pass
    #         # batch.commit()
    #         self.log.info(f'processed {count} {topic} docs in tenant {self.tenant}')
    #     except helpers.ClientException as ce:
    #         raise MessageHandlingException(
    #             'submission error',
    #             details={'offset': offset}
    #         ) from ce

    # thrown manually when something in _handle_messages goes wrong
    def _on_message_handle_exception(self, mhe: MessageHandlingException):
        LOG.error(f'msg_handle_exception: {mhe}')
        if str(mhe) == 'submission_error':
            LOG.debug(mhe.details)

    # called when a subscription causes a new assignment to be given to the consumer
    def _on_assign(self, *args, **kwargs):
        # TODO check rules for topic in Firebase
        assignment = args[1]
        for _part in assignment:
            if _part.topic not in self._previous_topics:
                self.log.info(f'New topic to configure: {_part.topic}')
                self._apply_consumer_filters(_part.topic)
                self._previous_topics.append(_part.topic)

    def _apply_consumer_filters(self, topic):
        self.log.debug(f'{self._id} applying filter for new topic {topic}')
        subscription = self._job_subscription_for_topic(topic)
        if not subscription:
            self.log.error(f'Could not find subscription for topic {topic}')
            return
        try:
            opts = subscription.definition.topic_options
            _flt = opts.get('filter_required', False)
            if _flt:
                _filter_options = {
                    'check_condition_path': opts.get('filter_field_path', ''),
                    'pass_conditions': opts.get('filter_pass_values', []),
                    'requires_approval': _flt
                }
                self.log.info(_filter_options)
                self.consumer.set_topic_filter_config(
                    topic,
                    FilterConfig(**_filter_options)
                )
            mask_annotation = opts.get('masking_annotation', None)
            if mask_annotation:
                _mask_options = {
                    'mask_query': mask_annotation,
                    'mask_levels': opts.get('masking_levels', []),
                    'emit_level': opts.get('masking_emit_level')
                }
                self.log.info(_mask_options)
                self.consumer.set_topic_mask_config(
                    topic,
                    MaskConfig(**_mask_options)
                )
            self.log.info(f'Filters applied for topic {topic}')
        except AttributeError as aer:
            self.log.error(f'No topic options for {subscription.id}| {aer}')

    def _name_from_topic(self, topic):
        return topic.lstrip(f'{self.tenant}.')

    # def _update_topic(self, resource: BQInstance, subscription: Subscription, topic, schema: Mapping[Any, Any]):
    #     if isinstance(resource, BigQuery):
    #         client = resource.get_bq()
    #         table = client._create_table()
    #         self.log.debug(f'{self.tenant} is updating topic schema: {topic},'
    #                        f' firebase does not care...')

    # public
    def list_topics(self, *args, **kwargs):
        '''
        Get a list of topics to which the job can subscribe.
        You can also use a wildcard at the end of names like:
        Name* which would capture both Name1 && Name2, etc
        '''
        timeout = 5
        try:
            md = self.consumer.list_topics(timeout=timeout)
        except (KafkaException) as ker:
            raise ConsumerHttpException(str(ker) + f'@timeout: {timeout}', 500)
        topics = [
            str(t).split(f'{self.tenant}.')[1] for t in iter(md.topics.values())
            if str(t).startswith(self.tenant)
        ]
        return topics

    # public
    def list_subscribed_topics(self, *arg, **kwargs):
        '''
        A List of topics currently subscribed to by this job
        '''
        return list(self.subscribed_topics.values())

    # public
    def list_assigned_topics(self, *arg, **kwargs):
        '''
        A List of topics currently assigned to this consumer
        '''
        return self._previous_topics[:]
