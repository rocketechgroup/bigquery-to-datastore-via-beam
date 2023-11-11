# BigQuery to Datastore via Beam

This is a sample project to show how to use Apache Beam to read data from BigQuery and write to Datastore in streaming
mode.

## How does this work

The pipeline is defined in `bigquery_to_datastore_via_beam/run.py`, there are a few key steps require for this to work

- Streaming mode: this is very important as the start time for dataflow is very long (5 minutes ~), and this won't be
  feasible if your use case is near real time. And the way to achieve this is via `PeriodicImpulse`
- Checkpointing: this is required to ensure delta tracking and only fetch what's required, the most important thing here
  is to never rely on time generate by different systems for comparison as time in a distributed environment is
  unreliable
- Datastore key: this is required to ensure the same record is not written to Datastore multiple times, this is done by
  using the primary key of the table as the key for Datastore
- BigQuery read: we are directly using the storage read API here as the read has to be unbounded to support streaming
  mode, plus the additional control required for checkpointing
- Datastore write: WriteToDatastore is used to write to Datastore, but make sure `throttle_rampup` is turned off due to
  the bug in this library

## Run locally

```
export PROJECT_ID=rocketech-de-pgcp-sandbox
gcloud storage cp config.yaml gs://${PROJECT_ID}-temp/bigquery-to-datastore-via-beam/config.yaml

python bigquery_to_datastore_via_beam/run.py --runner=DirectRunner --streaming \
    --pipeline_config_bucket ${PROJECT_ID}-temp \
    --pipeline_config_blob bigquery-to-datastore-via-beam/config.yaml \
    --project ${PROJECT_ID} \
    --region europe-west2 \
    --temp_location gs://${PROJECT_ID}-temp/bigquery-to-datastore-via-beam/dataflow/temp \
    --network private \
    --subnetwork regions/europe-west2/subnetworks/dataflow \
    --service_account_email dataflow@${PROJECT_ID}.iam.gserviceaccount.com \
    --max_num_workers 2 \
    --save_main_session 
   
```

## Run on Dataflow

To run on Dataflow, you need to package it as a template. See
a [video](https://www.youtube.com/watch?v=SnQ0Jx5nhoc&list=PL4RrzxkjhEKuZJKCjXrtzbECfnZQlp397&index=29) created by me
with a git repo attached for more details.
