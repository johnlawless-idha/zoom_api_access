import datetime
from multiprocessing import Value
import airflow
from airflow.models import Variable as var
from airflow.models.xcom import XCom
from airflow.utils import context
from airflow import DAG
from botocore.retries import bucket
# import sys
# sys.path.append('../functions/')
from botocore.vendored.six import python_2_unicode_compatible
import data_migration as dm
from airflow.operators.python import PythonOperator

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
            'bucket' = {{var.Value.bucket}},
            'prefix': 'zoom_api/transformed_dataframes/',
            'is_json': False
        }
    )

push_to_aws