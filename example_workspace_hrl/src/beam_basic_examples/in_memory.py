import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):
        def __init__(self, prefix=''):
            super().__init__()
            self.prefix = prefix

        def process(self, element):
            print(self.prefix+str(element))

    def __init__(self, label=None,prefix=''):
        super().__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._OutputFn(self.prefix))
 
def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--main_prefix',
        dest='main_prefix',
        required=True,
        help='Prefix for all output')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        (p
            | "Create elements" >> beam.Create(["Hello", "World!", "Beam"])
            | "Print elements" >> beam.Map(print))

        (p | 'Create words' >> beam.Create(['Hello Beam','It`s introduction'])
           | 'Log words' >> Output(prefix=known_args.main_prefix+".1-"))

        (p | 'Create numbers' >> beam.Create(range(1, 11))
           | 'Log numbers' >> Output(prefix=known_args.main_prefix+".2-"))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()