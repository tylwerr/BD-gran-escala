import argparse
import logging
import re
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
    return re.findall(r'[\w\']+', element, re.UNICODE)
  

class Output(beam.PTransform):
    def __init__(self, label=None,prefix=''):
        super().__init__(label)
        self.prefix = prefix

    def expand(self, input):
        (input 
            | beam.combiners.Count.Globally()
            | beam.Map(lambda x : self.prefix+str(x))
            | beam.Map(print))
 
def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file to process.')
    
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the file and split it into words
        lines = ( p 
         | "Read file" >> beam.io.ReadFromText(known_args.input) )

        words = ( lines
         | 'Split per word' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str)))
        

        (words 
             | 'Log word count' >> Output(label="word", prefix="word count: "))
        
        (lines 
             | 'Log lines count' >> Output(label="lines", prefix="lines count: "))
        

        def format_as_json(element):
            """Formats a key-value pair as a JSON string."""
            key, value = element
            return json.dumps({key: value})

        # Count the unique words
        (words 
             | 'Count words' >> beam.combiners.Count.PerElement()
             | 'FormatOutput' >> beam.Map(format_as_json)
             | beam.io.WriteToText(known_args.output, file_name_suffix='.txt'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()