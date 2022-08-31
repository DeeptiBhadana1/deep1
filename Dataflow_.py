import argparse
import logging
import re
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

PROJECT_ID = "marine-resource-352708"
tab_schema2 = parse_table_schema_from_json(json.dumps(json.load(open("Dataflow/tab_schema2.json"))))


def run(argv=None):
    """The main function which creates the pipeline and runs it"""

    parser = argparse.ArgumentParser() 
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    sql="""SELECT
                GENERATE_UUID() AS ID,
                Emp_ID,
                Father_s_Name AS Father_Name,
                Mother_s_Name AS Mother_Name,
                DATE_DIFF(CURRENT_DATE, COALESCE(SAFE.PARSE_DATE('%Y-%m-%d', Date_of_Birth), SAFE.PARSE_DATE('%d/%m/%Y', Date_of_Birth), SAFE.PARSE_DATE('%m/%d/%Y', Date_of_Birth)), year) AS Age_in_Yrs,
                Weight_in_Kgs_ AS Weight_in_Kgs,
                Phone_No_ AS Phone_No,
                State,
                Zip,
                Region,
                FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
            FROM `marine-resource-352708.Employee.employee_details`"""

 """Initiate the pipeline using the pipeline arguments passed in from the
    command line. This includes information such as the project ID and
    where Dataflow should store temp files"""

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))    
                          
    (
    p    
    """Read the file. This is the source of the pipeline. All further
       processing starts with lines read from the file. We use the input
       argument from the command line"""                              
     | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=sql, use_standard_sql=True))
     | 'Write to BigQuery' >> beam.io.Write(
                            beam.io.BigQuerySink('{0}:Employee.employee_personal_info'.format(PROJECT_ID),
                                                schema=tab_schema2,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))   
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()