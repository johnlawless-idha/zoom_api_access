import datetime
from multiprocessing import Value
import airflow
from airflow.models import Variable as var
from airflow.models.xcom import XCom
from airflow.utils import context
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.retries import bucket
import sys
sys.path.append('../functions/')
import data_migration as dm

#put default args here

with DAG(
    "push_raw_to_s3",
    start_date = datetime(2022,7,7),
    schedule_interval = '@daily',
    catchup = False    
) as dag:
    untransformed_df = PythonOperator(
        task_id = 'untransformed_df',
        value = context['task_instance'].xcom_pull(task_ids='df_mid_merge', dag_id = 'zoom_api_extraction'),
        python_callable= dm.upload_to_s3(),
        op_kwargs= {
            'data': value,
            'filename': 'untransformed_df',
            # Make a var bucket perhaps, if not, later you can import from secrets
            'bucket': {{var.Value.bucket}},
            'prefix': 'zoom_api/untransformed_dataframes/',
            'is_json': False
        }
    ),
    raw_participant_data = PythonOperator(
        task_id = 'raw_participants',
        value = context['task_instance'].xcom_pull(task_ids='get_meeting_participants', dag_id = 'zoom_api_extraction'),
        python_callable = dm.upload_to_s3(),
        op_kwargs= {
            'data': value,
            'filename': 'meeting_participants_raw',
            'bucket': {{var.Value.bucket}},
            'prefix': 'zoom_api/participants_json/',
            'is_json': True
        }        
    ),
    push_raw_qos = PythonOperator(
        task_id = 'raw_qos_push',
        value = context['task_instance'].xcom_pull(task_ids='get_raw_qos_data', dag_id = 'zoom_api_extraction'),
        python_callable = dm.upload_to_s3(),
        op_kwargs= {
            'data': value,
            'filename': 'meeting_metrics_raw',
            'bucket': {{var.Value.bucket}},
            'prefix': 'zoom_api/raw_metrics/',
            'is_json': True
        }             
    )

untransformed_df >> raw_participant_data >> push_raw_qos