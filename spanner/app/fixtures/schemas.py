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

SPANNER_INSTANCE = '''
{
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "required": [
    "id",
    "name",
    "instance",
    "database"
    "credential"
  ],
  "properties": {
    "id": {
      "$id": "#/properties/id",
      "type": "string",
      "title": "ID",
      "default": "",
      "examples": [
        "default"
      ],
      "pattern": "^(.*)$"
    },
    "name": {
      "$id": "#/properties/name",
      "type": "string",
      "title": "Name",
      "default": "",
      "examples": [
        "the default instance"
      ],
      "pattern": "^(.*)$"
    },
    "instance": {
      "$id": "#/properties/instance",
      "type": "string",
      "title": "Cloud Spanner InstanceID",
      "description": "Google Cloud Spanner InstanceID",
      "default": "",
      "examples": [
        "my-spanner-instance"
      ],
      "pattern": "^(.*)$"
    },
    "database": {
      "$id": "#/properties/database",
      "type": "string",
      "title": "Cloud Spanner DatabaseID",
      "description": "Google Cloud Spanner DatabaseID",
      "default": "",
      "examples": [
        "my-spanner-database"
      ],
      "pattern": "^(.*)$"
    },
    "credential": {
      "$id": "#/properties/credential",
      "type": "object",
      "title": "Google Credential",
      "description": "Google credentials json file as a string",
      "properties": {}
    }
  }
}
'''

BQ_INSTANCE = '''
{
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "bigquery",
  "type": "object",
  "title": "BigQuery",
  "required": [
    "id",
    "name",
    "dataset",
    "credential"
  ],
  "properties": {
    "id": {
      "$id": "#/properties/id",
      "type": "string",
      "title": "ID",
      "default": "",
      "examples": [
        "default"
      ],
      "pattern": "^(.*)$"
    },
    "name": {
      "$id": "#/properties/name",
      "type": "string",
      "title": "Name",
      "default": "",
      "examples": [
        "the default instance"
      ],
      "pattern": "^(.*)$"
    },
    "dataset": {
      "$id": "#/properties/dataset",
      "type": "string",
      "title": "Cloud Spanner InstanceID",
      "description": "Google BigQuery DatasetID",
      "default": "",
      "examples": [
        "my-data-set"
      ],
      "pattern": "^(.*)$"
    },
    "credential": {
      "$id": "#/properties/credential",
      "type": "object",
      "title": "Google Credential",
      "description": "Google credentials json file as a string",
      "properties": {}
    }
  }
}
'''

SUBSCRIPTION = '''
{
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "required": [
    "id",
    "name",
    "topic_pattern"
  ],
  "properties": {
    "id": {
      "$id": "#/properties/id",
      "type": "string",
      "title": "The Id Schema",
      "default": "",
      "examples": [
        "the id for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "name": {
      "$id": "#/properties/name",
      "type": "string",
      "title": "The Name Schema",
      "default": "",
      "examples": [
        "a nice name for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "table": {
      "$id": "#/properties/table",
        "type": "string",
        "title": "Destination Table",
        "default": "",
        "examples": [
          "my-table"
        ],
        "pattern": "^(.*)$"
    },
    "topic_pattern": {
      "$id": "#/properties/topic_pattern",
      "type": "string",
      "title": "The Topic_pattern Schema",
      "default": "",
      "examples": [
        "source topic for data i.e. gather*"
      ],
      "pattern": "^(.*)$"
    },
    "topic_options": {
      "$id": "#/properties/topic_options",
      "type": "object",
      "title": "The Topic_options Schema",
      "anyOf": [
        {
          "required": [
            "masking_annotation"
          ]
        },
        {
          "required": [
            "filter_required"
          ]
        }
      ],
      "dependencies": {
        "filter_required": [
          "filter_field_path",
          "filter_pass_values"
        ],
        "masking_annotation": [
          "masking_levels",
          "masking_emit_level"
        ]
      },
      "properties": {
        "masking_annotation": {
          "$id": "#/properties/topic_options/properties/masking_annotation",
          "type": "string",
          "title": "The Masking_annotation Schema",
          "default": "",
          "examples": [
            "@aether_masking"
          ],
          "pattern": "^(.*)$"
        },
        "masking_levels": {
          "$id": "#/properties/topic_options/properties/masking_levels",
          "type": "array",
          "title": "The Masking_levels Schema",
          "items": {
            "$id": "#/properties/topic_options/properties/masking_levels/items",
            "title": "The Items Schema",
            "examples": [
              "private",
              "public"
            ],
            "pattern": "^(.*)$"
          }
        },
        "masking_emit_level": {
          "$id": "#/properties/topic_options/properties/masking_emit_level",
          "type": "string",
          "title": "The Masking_emit_level Schema",
          "default": "",
          "examples": [
            "public"
          ],
          "pattern": "^(.*)$"
        },
        "filter_required": {
          "$id": "#/properties/topic_options/properties/filter_required",
          "type": "boolean",
          "title": "The Filter_required Schema",
          "default": false,
          "examples": [
            false
          ]
        },
        "filter_field_path": {
          "$id": "#/properties/topic_options/properties/filter_field_path",
          "type": "string",
          "title": "The Filter_field_path Schema",
          "default": "",
          "examples": [
            "some.json.path"
          ],
          "pattern": "^(.*)$"
        },
        "filter_pass_values": {
          "$id": "#/properties/topic_options/properties/filter_pass_values",
          "type": "array",
          "title": "The Filter_pass_values Schema",
          "items": {
            "$id": "#/properties/topic_options/properties/filter_pass_values/items",
            "title": "The Items Schema",
            "examples": [
              false
            ]
          }
        }
      }
    }
  }
}
'''

SPANNER_JOB = '''
{
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "SpannerJob",
  "oneOf": [
    {
      "required": [
        "id",
        "name",
        "spanner"
      ]
    },
    {
      "required": [
        "id",
        "name",
        "bigquery"
      ]
    }
  ],
  "properties": {
    "id": {
      "$id": "#/properties/id",
      "type": "string",
      "title": "ID",
      "default": "",
      "examples": [
        "the id for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "name": {
      "$id": "#/properties/name",
      "type": "string",
      "title": "Name",
      "default": "",
      "examples": [
        "a nice name for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "spanner": {
      "$id": "#/properties/spanner",
      "type": "string",
      "title": "SpannerInstance",
      "description": "The CloudSpanner Resource",
      "default": "",
      "examples": [
        "my-spanner"
      ],
      "pattern": "^(.*)$"
    },
    "bigquery": {
      "$id": "#/properties/bigquery",
      "type": "string",
      "title": "BigQueryInstance",
      "description": "The BigQuery Resource",
      "default": "",
      "examples": [
        "my-bq"
      ],
      "pattern": "^(.*)$"
    },
    "subscription": {
      "$id": "#/properties/subscription",
      "type": "array",
      "title": "Subscription",
      "items": {
        "$id": "#/properties/subscription/items",
        "type": "string",
        "title": "The Items Schema",
        "default": "",
        "examples": [
          "id-of-sub"
        ],
        "pattern": "^(.*)$"
      }
    }
  }
}
'''
