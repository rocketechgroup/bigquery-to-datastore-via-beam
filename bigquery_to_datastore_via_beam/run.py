import argparse
import logging
import yaml
import apache_beam as beam

from datetime import datetime, date as datetime_date

from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from google.cloud.bigquery_storage import BigQueryReadClient, types
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.io.gcp.datastore.v1new.types import Entity, Key
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def read_yaml_from_gcs(bucket_name, blob_name):
    """Reads a YAML file from a GCS bucket and parses it into a dictionary."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Read the blob's content into an in-memory string
    yaml_string = blob.download_as_text()

    # Parse the YAML string into a Python dictionary
    yaml_content = yaml.safe_load(yaml_string)

    return yaml_content


def create_pipeline(pipeline_options, pipeline_config, impulse_start_timestamp, impulse_fire_interval, project_id):
    checkpointing_table_id = pipeline_config['checkpointing_table_id']
    pipeline_name = pipeline_config['name']
    source_table_id = pipeline_config['source_table_id']
    source_timestamp_column = pipeline_config['source_timestamp_column']
    datastore_key_column = pipeline_config['datastore_key_column']
    datastore_namespace = pipeline_config['datastore_namespace']
    datastore_kind = pipeline_config['datastore_kind']
    source_table_columns = pipeline_config['source_table_columns']

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
                pipeline
                | f'Start Impulse {pipeline_name}' >> PeriodicImpulse(start_timestamp=impulse_start_timestamp,
                                                                      stop_timestamp=MAX_TIMESTAMP,
                                                                      fire_interval=impulse_fire_interval)
                | f"Get Filters for BigQuery {pipeline_name}" >> beam.ParDo(GetFiltersForBigQuery(),
                                                                            source_table_id=source_table_id,
                                                                            source_timestamp_column=source_timestamp_column,
                                                                            checkpointing_table_id=checkpointing_table_id,
                                                                            impulse_start_timestamp=impulse_start_timestamp,
                                                                            impulse_fire_interval=impulse_fire_interval)

                | f"Fetch delta from BigQuery {pipeline_name}" >> beam.ParDo(FetchDeltaFromBigQuery(),
                                                                             project_id=project_id,
                                                                             source_table_id=source_table_id,
                                                                             source_table_columns=source_table_columns)
                | f"Convert to Datastore Entity {pipeline_name}" >> beam.ParDo(ConvertToDatastoreEntity(),
                                                                               datastore_key_column=datastore_key_column,
                                                                               datastore_namespace=datastore_namespace,
                                                                               datastore_kind=datastore_kind,
                                                                               project_id=project_id)
                | f"Write to Datastore {pipeline_name}" >> WriteToDatastore(project_id, throttle_rampup=False)
        )


class ConvertToDatastoreEntity(beam.DoFn):
    def process(self, record, datastore_namespace, datastore_kind, datastore_key_column, project_id):
        # Convert BigQuery record to Cloud Datastore entity
        key = Key(
            path_elements=[datastore_kind, str(record[datastore_key_column])],
            namespace=datastore_namespace,
            project=project_id
        )
        entity = Entity(key=key)
        # add the rest of the fields
        for k, value in record.items():
            value = value.as_py()  # convert arrow format to native python format
            if isinstance(value, datetime_date):
                value = value.strftime('%Y-%m-%d')
            entity.set_properties({k: value})

        yield entity


class GetFiltersForBigQuery(beam.DoFn):
    def process(self, impulse_timestamp, source_table_id, source_timestamp_column, checkpointing_table_id,
                impulse_start_timestamp, impulse_fire_interval):

        def fetch_max_timestamp_from_source():
            query = f"""
                    SELECT max({source_timestamp_column}) as max_timestamp FROM `{source_table_id}`
                    """
            job = client.query(query)
            results = job.result()
            row = next(iter(results))
            if row['max_timestamp']:
                result = row['max_timestamp']
            else:
                result = datetime.min  # no data in the table

            return result

        def fetch_last_incomplete_timestamp():
            query = f"""
                    SELECT max(last_incomplete_timestamp) as last_incomplete_timestamp FROM `{checkpointing_table_id}`
                    WHERE table_id = '{source_table_id}'
                    """
            job = client.query(query)
            results = job.result()
            row = next(iter(results))
            if row['last_incomplete_timestamp']:
                result = row['last_incomplete_timestamp']
            else:
                result = datetime.min

            return result

        logging.info('impulse_timestamp:' + str(impulse_timestamp))
        logging.info('impulse_start_timestamp:' + str(impulse_start_timestamp))
        client = bigquery.Client()

        try:
            # try to get the `from_timestamp` from the checkpointing table
            from_timestamp = fetch_last_incomplete_timestamp()
        except NotFound as e:
            # create the table if it's not there.
            query = f"""
                        CREATE TABLE `{checkpointing_table_id}` (
                            table_id STRING,
                            last_processed_timestamp TIMESTAMP,
                            last_incomplete_timestamp TIMESTAMP,
                            row_updated_timestamp TIMESTAMP
                        );
                        """
            job = client.query(query)
            job.result()
            from_timestamp = datetime.min

        # work out the `to_timestamp` and to timestamps
        to_timestamp = fetch_max_timestamp_from_source()

        logging.info('from_timestamp:' + str(from_timestamp))
        logging.info('to_timestamp:' + str(to_timestamp))

        # if there is no checkpoint, insert a checkpoint
        from_timestamp_iso_m = from_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
        to_timestamp_iso_m = to_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
        if from_timestamp == datetime.min:
            query = f"""
                    INSERT INTO `{checkpointing_table_id}` 
                        (table_id, last_processed_timestamp, last_incomplete_timestamp, row_updated_timestamp)
                    VALUES 
                        ('{source_table_id}', '{from_timestamp_iso_m}', '{to_timestamp_iso_m}', CURRENT_TIMESTAMP())
                    """
            job = client.query(query)
            job.result()
        else:  # or update the checkpoint
            query = f"""
                    UPDATE `{checkpointing_table_id}` 
                    SET last_processed_timestamp = '{from_timestamp_iso_m}', 
                        last_incomplete_timestamp = '{to_timestamp_iso_m}', 
                    row_updated_timestamp = CURRENT_TIMESTAMP()
                    WHERE table_id = '{source_table_id}'
                    """
            job = client.query(query)
            job.result()

        filters = f"{source_timestamp_column} BETWEEN " \
                  f"TIMESTAMP('{from_timestamp_iso_m}') AND TIMESTAMP('{to_timestamp_iso_m}')"

        yield filters


class FetchDeltaFromBigQuery(beam.DoFn):
    def process(self, filters, project_id, source_table_id, source_table_columns):
        logging.info("Fetching delta from BigQuery:" + str(filters))
        client = BigQueryReadClient()
        requested_session = types.stream.ReadSession()
        dataset_id, table_id = source_table_id.split('.')
        requested_session.table = f'projects/{project_id}/datasets/{dataset_id}/tables/{table_id}'
        requested_session.data_format = types.stream.DataFormat.ARROW
        requested_session.read_options.selected_fields = source_table_columns
        requested_session.read_options.row_restriction = f'{filters}'
        parent = "projects/{}".format(project_id)
        session = client.create_read_session(
            parent=parent,
            read_session=requested_session,
            max_stream_count=1
        )
        if session.streams:
            reader = client.read_rows(session.streams[0].name)
            rows = reader.rows(session)
            for row in rows:
                yield row
        else:
            logging.info("No delta to process")


def run(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline_config_bucket', required=True, help='GCS bucket where pipeline config is stored')
    parser.add_argument('--pipeline_config_blob', required=True, help='Path to the blob in the GCS bucket')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_config_bucket = known_args.pipeline_config_bucket
    pipeline_config_blob = known_args.pipeline_config_blob

    pipeline_options = PipelineOptions(pipeline_args)
    project_id = pipeline_options.get_all_options()['project']

    impulse_start_timestamp = datetime.utcnow().timestamp()
    impulse_fire_interval = 30  # seconds

    pipeline_config = read_yaml_from_gcs(bucket_name=pipeline_config_bucket,
                                         blob_name=pipeline_config_blob)

    logging.info('Starting pipeline..')

    create_pipeline(pipeline_options=pipeline_options,
                    pipeline_config=pipeline_config,
                    impulse_start_timestamp=impulse_start_timestamp,
                    impulse_fire_interval=impulse_fire_interval,
                    project_id=project_id)


if __name__ == '__main__':
    run()
