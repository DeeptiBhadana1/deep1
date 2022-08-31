#1
from airflow import DAG
#from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
#from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator

#2
dag = DAG(dag_id= 'DAG-1', start_date=datetime(2022,8,9), catchup=False, schedule_interval='@once')
start = DummyOperator(task_id='start'),
end = DummyOperator(task_id='end')

#3
csv_to_bigquery = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    gcp_conn_id='google_cloud_default',
    bucket="my_first_bucket_01_first",
    source_objects=['100 Records.csv'],
    skip_leading_rows=1,
    bigquery_conn_id='google_cloud_default',    
    destination_project_dataset_table="marine-resource-352708.Details.ONLINE_RETAIL_RAW",
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    #schema_update_options=['ALLOW_FIELD_RELAXATION', 'ALLOW_FIELD_ADDITION'],
    autodetect=True,
    dag=dag
)

start >> csv_to_bigquery >>end