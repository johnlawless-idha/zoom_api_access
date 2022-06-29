import datetime
import airflow
from airflow.models import Variable as var
from airflow.models.xcom import XCom
from airflow.utils import context
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append('../functions/')
import data_migration as dm

#put default args here

#This will later include a task to migrate to microstrategy as well
with DAG(
    "push_raw_to_s3",
    start_date = datetime(2022,7,7),
    schedule_interval = '@daily',
    catchup = False    
) as dag:
    push_to_aws = PythonOperator(
        task_id = 'push_to_s3',
        df = context['task_instance'].xcom_pull(task_ids = 'generate_final_features', dag_id = 'transform_to_df'),
        python_callable = dm.upload_to_s3(),
        op_kwargs = {
            'data':df,
            'filename':'zoom_data_df_transformed',
            'bucket' : {{var.Value.bucket}},
            'prefix': 'zoom_api/transformed_dataframes/',
            'is_json': False
        }
    )

push_to_aws