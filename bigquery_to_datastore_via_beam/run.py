import argparse
import logging
import apache_beam as beam

from datetime import datetime, date as datetime_date

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from google.cloud.bigquery_storage import BigQueryReadClient, types
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.io.gcp.datastore.v1new.types import Entity, Key
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class ConvertToDatastoreEntity(beam.DoFn):
    def process(self, record, datastore_namespace, datastore_kind, datastore_key_column, project_id):
        logging.info("ConvertToDatastoreEntity:" + str(record))
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
        def fetch_last_processed_timestamp():
            query = f"""
                    SELECT max(last_processed_timestamp) as last_processed_timestamp FROM `{checkpointing_table_id}`
                    WHERE table_id = '{source_table_id}'
                    """
            job = client.query(query)
            results = job.result()
            row = next(iter(results))
            if row['last_processed_timestamp']:
                result = row['last_processed_timestamp']
            else:
                result = datetime.min

            return result

        logging.info('impulse_timestamp:' + str(impulse_timestamp))
        logging.info('impulse_start_timestamp:' + str(impulse_start_timestamp))
        client = bigquery.Client()

        last_processed_timestamp = None
        if impulse_start_timestamp == impulse_timestamp:  # first time the job runs
            """
            Check if there is a checkpoint in the database, if there is no checkpoint, fetch the whole table.
            If there is a checkpoint in the database, it means that the job has run before, so we need to fetch the
            checkpoint timestamp and use it as the start timestamp for the query.
            
            We should never update the checkpoint in the database when the job is running for the first time. If we do
            and the job fails, we've already updated the checkpoint and we'll lose data. 
            """

            try:
                last_processed_timestamp = fetch_last_processed_timestamp()
            except NotFound as e:
                # create the table if it's not there.
                query = f"""
                            CREATE TABLE `{checkpointing_table_id}` (
                                table_id STRING,
                                last_processed_timestamp TIMESTAMP,
                                row_updated_timestamp TIMESTAMP
                            );
                            """
                job = client.query(query)
                job.result()
                last_processed_timestamp = datetime.min

        else:
            """
            This is the second or subsequent time the job is running. We need to first update the checkpoint using 
            (impulse_timestamp - the interval) = the last processed timestamp. This ensures that we don't miss any
            data between the `start` of the previous impulse and the `start` of the current impulse. 
            """
            last_processed_timestamp = datetime.fromtimestamp(impulse_timestamp - impulse_fire_interval)
            last_processed_timestamp_from_db = fetch_last_processed_timestamp()
            if last_processed_timestamp_from_db == datetime.min:
                query = f"""
                        INSERT INTO `{checkpointing_table_id}` (table_id, last_processed_timestamp, row_updated_timestamp)
                        VALUES ('{source_table_id}', '{last_processed_timestamp.isoformat()}', CURRENT_TIMESTAMP())
                        """
                job = client.query(query)
                job.result()
            else:
                query = f"""
                        UPDATE `{checkpointing_table_id}` 
                        SET last_processed_timestamp = '{last_processed_timestamp.isoformat()}', 
                        row_updated_timestamp = CURRENT_TIMESTAMP()
                        WHERE table_id = '{source_table_id}'
                        """
                job = client.query(query)
                job.result()

        filters = f"{source_timestamp_column} > TIMESTAMP('{last_processed_timestamp.isoformat()}') "

        yield filters


class FetchDeltaFromBigQuery(beam.DoFn):
    def process(self, filters, project_id, source_table_id):
        logging.info("FetchDeltaFromBigQuery:" + str(filters))
        client = BigQueryReadClient()
        requested_session = types.stream.ReadSession()
        dataset_id, table_id = source_table_id.split('.')
        requested_session.table = f'projects/{project_id}/datasets/{dataset_id}/tables/{table_id}'
        requested_session.data_format = types.stream.DataFormat.ARROW
        # TODO: refactor to pass in dynamically
        requested_session.read_options.selected_fields = ['uuid', 'full_name', 'email', 'address', 'phone_number',
                                                          'birthdate', 'last_updated_timestamp']
        requested_session.read_options.row_restriction = f'{filters}'
        parent = "projects/{}".format(project_id)
        session = client.create_read_session(
            parent=parent,
            read_session=requested_session,
            max_stream_count=1
        )
        reader = client.read_rows(session.streams[0].name)
        rows = reader.rows(session)
        for row in rows:
            yield row


def run(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_table_id', required=True)  # dataset.table_id
    parser.add_argument('--source_timestamp_column', required=True)
    parser.add_argument('--checkpointing_table_id', required=True)  # dataset.table_id
    parser.add_argument('--datastore_key_column', required=True)  # column name of the bigquery table for datastore key
    parser.add_argument('--datastore_namespace', required=True)
    parser.add_argument('--datastore_kind', required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    source_table_id = known_args.source_table_id
    source_timestamp_column = known_args.source_timestamp_column
    checkpointing_table_id = known_args.checkpointing_table_id

    datastore_key_column = known_args.datastore_key_column
    datastore_namespace = known_args.datastore_namespace
    datastore_kind = known_args.datastore_kind

    pipeline_options = PipelineOptions(pipeline_args)
    project_id = pipeline_options.get_all_options()['project']

    impulse_start_timestamp = datetime.utcnow().timestamp()
    impulse_fire_interval = 30  # seconds

    logging.info('Starting pipeline')

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
                pipeline
                | 'Start Impulse' >> PeriodicImpulse(start_timestamp=impulse_start_timestamp,
                                                     stop_timestamp=MAX_TIMESTAMP, fire_interval=impulse_fire_interval)
                | "Get Filters for BigQuery" >>
                beam.ParDo(GetFiltersForBigQuery(),
                           source_table_id=source_table_id,
                           source_timestamp_column=source_timestamp_column,
                           checkpointing_table_id=checkpointing_table_id,
                           impulse_start_timestamp=impulse_start_timestamp,
                           impulse_fire_interval=impulse_fire_interval)

                | "Fetch delta from BigQuery" >> beam.ParDo(FetchDeltaFromBigQuery(),
                                                            project_id=project_id,
                                                            source_table_id=source_table_id)
                | "Convert to Datastore Entity" >> beam.ParDo(ConvertToDatastoreEntity(),
                                                              datastore_key_column=datastore_key_column,
                                                              datastore_namespace=datastore_namespace,
                                                              datastore_kind=datastore_kind,
                                                              project_id=project_id)
                | "Write to Datastore" >> WriteToDatastore(project_id, throttle_rampup=False)
        )


if __name__ == '__main__':
    run()
