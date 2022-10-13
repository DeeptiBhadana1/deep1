from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

dag=DAG(dag_id='DAG-bash1', schedule_interval='@once', start_date=datetime.today(), catchup=False)

# Task 1
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Loading CSV from GCS to BQ
# Create a dataset (customer), then table data_raw has been created.
# Loading data.csv from bucket to bq
# bq query will run query1.sql file
bash_cmd = """  bq --location us-central1 mk customer
                bq load \ 
                    --replace=true\
                    --source_format=CSV \
                    --skip_leading_rows=1 \
                    customer.data_raw \
                    gs://deepti_bucket1/data.csv \
                    InvoiceNo:STRING,StockCode:STRING,Description:STRING,Quantity:FLOAT,InvoiceDate:STRING,UnitPrice:FLOAT,CustomerID:STRING,Country:STRING 
                bq query \
                    --batch \
                    --replace=true \
                    --use_legacy_sql=false \
                    < /home/airflow/gcs/dags/final.sql
            """

gcs_to_bq= BashOperator(task_id='gcs_to_bq', bash_command=bash_cmd, dag=dag)


start >> gcs_to_bq >> end
