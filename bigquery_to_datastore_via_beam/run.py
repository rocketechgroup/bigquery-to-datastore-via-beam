import argparse
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (p | 'ReadLines' >> beam.Create(["Hello", "World"])
         | 'WriteResults' >> beam.io.WriteToText(known_args.output))


if __name__ == '__main__':
    run()
