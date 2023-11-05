import argparse
import apache_beam as beam
import pendulum

from apache_beam.options.pipeline_options import PipelineOptions

# Must use fully qualified package name, `from library` won't work
from bigquery_to_datastore_via_beam.library import sample_input


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (p | 'ReadLines' >> beam.Create(sample_input)
         | 'AddTimestamps' >> beam.Map(lambda x: str(x) + ":" + pendulum.now().to_iso8601_string())
         | 'WriteResults' >> beam.io.WriteToText(known_args.output))


if __name__ == '__main__':
    run()
