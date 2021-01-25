# aether-cloud-spanner-consumer

## Resources

### /bigquery
```
{
    'id': 'default',
    'name': 'the default instance',
    'project': 'some-project',                  # Project ID of BigQuery instance
    'credential': {'json': 'doc'},              # service account credentials document as JSON
    'dataset': 'test_ds'                        # Dataset name in BQ Project
}
```
#### additional endpoints
 - /bigquery/test_connection?id={resource_id}

### /subscription

Describes a source Kafka topic and destination table. The example `topic_options` are safe, and should be used if you don't need advanced filtering. 

```
{
    'id': 'sub-test',
    'name': 'Test Subscription',
    'table': 'test_table',                        # Output table to write to
    'topic_pattern': '*',                         # kafka topic 
    'topic_options': {
        'masking_annotation': '@aether_masking',  # schema key for mask level of a field
        'masking_levels': ['public', 'private'],  # classifications
        'masking_emit_level': 'public',           # emit from this level ->
        'filter_required': False
    }
}
```

## Job
### /job
```
{
    'id': 'default',
    'name': 'Default Google Consumer Job',
    'bigquery': 'default',
    'subscription': ['sub-test']
}
```
#### additional endpoints
 - /job/list_topics?id={job_id}
 - /job/list_subscribed_topics?id={job_id}
 - /job/list_assigned_topics?id={job_id}
 