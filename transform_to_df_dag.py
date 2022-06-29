import datetime
from multiprocessing import Value
import airflow
from airflow.models import Variable as var
from airflow.models.xcom import XCom
from airflow.utils import context
from airflow import DAG
from botocore.retries import bucket
from airflow.operators.python import PythonOperator
import sys
sys.path.append('../functions/')
import data_migration as dm
import df_transformation as dt

#put default args here

with DAG(
    "transform_to_df",
    start_date = datetime(2022,7,7),
    schedule_interval = '@daily',
    catchup = False    
) as dag:
    retrieve_untransformed_df = PythonOperator(
        task_id = 'untransformed_df',
        python_callable=dm.retrieve_s3_to_csv(),
        op_kwargs= {
            'Bucket': {{var.Value.bucket}},
            'Key':'zoom_api/untransformed_dataframes/untransformed_df.csv'
        }
    ),
    retrieve_raw_qos = PythonOperator(
        task_id = 'get_raw_qos',
        python_callable=dm.retrieve_s3_to_json(),
        op_kwargs={
            'Bucket':{{var.Value.bucket}},
            'Key':'zoom_api/raw_metrics/meeting_metrics_raw.json'
        }
    ),
    convert_qos_to_df = PythonOperator(
        task_id = 'convert_raw_to_df',
        df_orig = context['task_instance'].xcom_pull(task_ids = 'untransformed_df'),
        raw_qos = context['task_instance'].xcom_pull(task_ids = 'get_raw_qos'),
        python_callable=dt.convert_json_qos_to_df(),
        op_kwargs={
            'json_users':raw_qos,
            'meeting_df':df_orig
        }
    ),
    #These COULD be another dag as well, but they follow the same basic action of transforming the df for microstrategy
    disconnect_cols = PythonOperator(
        task_id = 'generate_disconnect_cols',
        main_df = context['task_instance'].xcom_pull(task_ids = 'convert_raw_to_df'),
        python_callable = dt.generate_disconnect_cols(),
        op_kwargs = {
            'df':main_df
        }
    ),
    user_host_locator = PythonOperator(
        task_id = 'host_locator',
        main_df = context['task_instance'].xcom_pull(task_ids = 'generate_disconnect_cols'),
        python_callable=dt.host_locator(),
        op_kwargs={
            'df': main_df
        }
    ),
    create_final_features = PythonOperator(
        task_id = 'generate_final_features',
        main_df = context['task_instance'].xcom_pull(task_ids = 'host_locator'),
        python_callable = dt.generate_df_features(),
        op_kwargs={
            'in_df':main_df
        }
    )
retrieve_untransformed_df >> retrieve_raw_qos >> convert_qos_to_df >> disconnect_cols >> user_host_locator >> create_final_features