import argparse
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


class QueryForFetchingDeltaFromBigQuery(beam.DoFn):
    def process(self, element, source_table_id, source_timestamp_column, checkpointing_table_id):
        """
            Get the query required to fetch data from BigQuery.
            Fetch the whole table if no previous checkpointing timestamp is found, otherwise fetch the delta from the
            previous checkpointing timestamp.
            :param source_table_id: source table id used to fetch the delta
            :param source_timestamp_column: timestamp column used to fetch the delta
            :param checkpointing_table_id: checkpointing table id used to store the last timestamp
            :return: query to fetch the delta
            """
        client = bigquery.Client()

        # Try to fetch the last checkpointing timestamp
        last_processed_timestamp = None
        try:
            query = f"""
                        SELECT max(last_processed_timestamp) as last_processed_timestamp FROM `{checkpointing_table_id}`
                        WHERE table_id = '{source_table_id}'
                        """
            job = client.query(query)
            results = job.result()
            row = next(iter(results))
            if row['last_processed_timestamp']:
                last_processed_timestamp = row['last_processed_timestamp']
            else:
                last_processed_timestamp = datetime.min

        except NotFound as e:
            print(e)
            # create the table if it's not there.
            query = f"""
                CREATE TABLE `{checkpointing_table_id}` (
                    uuid STRING,
                    table_id STRING,
                    last_processed_timestamp TIMESTAMP
                );
                """
            job = client.query(query)
            job.result()

        query = f"""
            SELECT * FROM `{source_table_id}`
            WHERE {source_timestamp_column} > TIMESTAMP('{last_processed_timestamp}')
            """

        yield query


class FetchDeltaFromBigQuery(beam.DoFn):
    def process(self, query, project_id, source_table_id):
        print("FetchDeltaFromBigQuery:" + str(query))
        client = BigQueryReadClient()
        requested_session = types.stream.ReadSession()
        dataset_id, table_id = source_table_id.split('.')
        requested_session.table = f'projects/{project_id}/datasets/{dataset_id}/tables/{table_id}'
        requested_session.data_format = types.stream.DataFormat.ARROW
        # TODO: refactor to pass in dynamically
        requested_session.read_options.selected_fields = ['uuid', 'full_name', 'email', 'address', 'phone_number',
                                                          'birthdate', 'last_updated_timestamp']
        requested_session.read_options.row_restriction = 'last_updated_timestamp > TIMESTAMP("0001-01-01 00:00:00")'
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
    parser = argparse.ArgumentParser()
    parser.add_argument('--subscription_name', required=True)  # subscription_name of the pubsub topic as the trigger
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

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
                pipeline
                | 'Start Impulse' >> PeriodicImpulse(start_timestamp=datetime.utcnow().timestamp(),
                                                     stop_timestamp=MAX_TIMESTAMP, fire_interval=30)
                | 'Trigger' >> beam.Map(lambda x: print("Impulse:" + str(x)))
                | "Get the Query for Delta from BigQuery" >> beam.ParDo(QueryForFetchingDeltaFromBigQuery(),
                                                                        source_table_id=source_table_id,
                                                                        source_timestamp_column=source_timestamp_column,
                                                                        checkpointing_table_id=checkpointing_table_id)
                | "Fetch delta from BigQuery" >> beam.ParDo(FetchDeltaFromBigQuery(), project_id=project_id,
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
