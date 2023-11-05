import argparse
import apache_beam as beam

from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from google.cloud.datastore_v1 import Entity
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class ConvertToDatastoreEntity(beam.DoFn):
    def process(self, record):
        # Convert BigQuery record to Cloud Datastore entity
        entity = Entity()
        entity.key.partition_id.project_id = 'YOUR_PROJECT_ID'
        # You can add more attributes to the entity here
        entity.properties['some_field'].string_value = record['some_field']
        yield entity


def get_query_to_fetch_from_bigquery(source_table_id, source_timestamp_column, checkpointing_table_id):
    """
    Get the query required to fetch data from BigQuery.
    Fetch the whole table if no previous checkpointing timestamp is found, otherwise fetch the delta from the
    previous checkpointing timestamp.
    :param element: Not used
    :param source_table_id: source table id used to fetch the delta
    :param source_timestamp_column: timestamp column used to fetch the delta
    :param checkpointing_table_id: checkpointing table id used to store the last timestamp
    :return: delta records
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

    return query


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_table_id', required=True)  # dataset.table_id
    parser.add_argument('--source_timestamp_column', required=True)
    parser.add_argument('--checkpointing_table_id', required=True)  # dataset.table_id
    known_args, pipeline_args = parser.parse_known_args(argv)

    source_table_id = known_args.source_table_id
    source_timestamp_column = known_args.source_timestamp_column
    checkpointing_table_id = known_args.checkpointing_table_id

    pipeline_options = PipelineOptions(pipeline_args)

    query_to_fetch_delta = get_query_to_fetch_from_bigquery(source_table_id, source_timestamp_column,
                                                            checkpointing_table_id)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         # | "Start periodic Impulse" >> PeriodicImpulse(start_timestamp=datetime.utcnow().timestamp(),
         #                                               fire_interval=60 * 5,
         #                                               stop_timestamp=MAX_TIMESTAMP
         #                                               )

         | "Fetch Delta from BigQuery" >> beam.io.ReadFromBigQuery(query=query_to_fetch_delta, use_standard_sql=True)
         | 'Print the results' >> beam.Map(print)
         )


if __name__ == '__main__':
    run()
