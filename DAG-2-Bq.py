#1
from airflow import DAG
#from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
#from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator

#2
dag = DAG(dag_id= 'DAG-2-Bq', start_date=datetime(2022,8,30), catchup=False, schedule_interval='@once')
start = DummyOperator(task_id='start'),
end = DummyOperator(task_id='end')

#3
sql1="""SELECT Emp_ID, First_Name, Gender, E_Mail, Date_of_Birth, Date_of_Joining, Age_in_Company__Years_, Salary, current_datetime() created_time, current_datetime() modified_time, DATE_DIFF(CURRENT_DATE, CAST(Date_of_Birth AS DATE), year) AS Age_in_Yrs, CONCAT(First_Name , ' ', Last_Name) as Name FROM `marine-resource-352708.Details.ONLINE_RETAIL_RAW`"""

#4
csv_to_bigquery = BigQueryOperator(
    task_id='create_employee_table',
    sql=sql1,
    destination_dataset_table='marine-resource-352708.Details.employee',
    #source_format='CSV',   
    use_legacy_sql=False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

#5
sql2="""SELECT Emp_ID, Father_s_Name, Mother_s_Name, Age_in_Yrs_, Weight_in_Kgs_,Phone_No__ ,State,Zip, Region, current_datetime() created_time, current_datetime() modified_time  FROM `marine-resource-352708.Details.ONLINE_RETAIL_RAW`"""

#6
csv_to_bigquery1 = BigQueryOperator(
    task_id='create_employee_personal_info_table',
    sql=sql2,
    destination_dataset_table='marine-resource-352708.Details.employee_personal_info',
    #source_format='CSV',
    use_legacy_sql=False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

start >> csv_to_bigquery >> csv_to_bigquery1 >>end