import datetime
import airflow
from airflow.models import Variable as var
from airflow.models.xcom import XCom
from airflow.utils import context
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append('../functions/')
#Can delete data_migration later, if push to s3 is done in a separate dag
import data_migration as dm
import zoom_api_data_extraction as ext


#Note: is_test can be a defined variable within airflow, set to True or False without needing to alter existing script.
#Make sure to define it as a variable within airflow! Also, include "bucket" for later pushes to S3 (or retrieve from secrets)

#Put default args here

with DAG(
    "zoom_api_extraction",
    start_date = datetime(2022, 7, 7),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    create_meetings = PythonOperator(
        task_id = 'create_meeting_df',
        python_callible = ext.get_meetings(),
        op_kwargs = {
            'gen_list': True,
            'user_df': None,
            'end_date': datetime.date.today(),
            'start_date': None,
            'is_test': {{var.value.is_test}},
            'return_json':False
        },
    ),
    get_participants = PythonOperator(
        task_id = 'get_meeting_participants',
        #This should retrieve the return_value that gets auto pushed according to docs, double check this syntax
        #value = task_instance.xcom_pull(task_ids = 'create_meeting_df'),
        #This SHOULD default to key = 'return_value', which should be default output of task
        value = context['task_instance'].xcom_pull(task_ids='create_meeting_df'), 
        python_callable = ext.get_participants(),
        op_kwargs = {
            'meeting_df': value,
            'return_json': True,
            'is_test': {{var.value.is_test}}
            
        }
    ),

    mid_merge = PythonOperator(
        task_id = 'df_mid_merge',
        #This might return a dictionary object, in which case you can call context once with both ids, then reference
        #those specific keys in op_kwargs (value[key1])
        value1 = context['task_instance'].xcom_pull(task_ids='create_meeting_df'),
        value2 = context['task_instance'].xcom_pull(task_ids='get_meeting_participants'),
        python_callable= ext.convert_json_particpants_to_csv(),
        op_kwargs= {
            'data':value2,
            'base_df':value1
        }
    ),

    get_raw_qos = PythonOperator(
        task_id = 'get_raw_qos_data',
        value = context['task_instance'].xcom_pull(task_ids='create_meeting_df'),
        python_callable = ext.qos_data(), 
        op_kwargs = {
            'meeting_df': value,
            'return_json': True,
            'is_test': {{var.Value.is_test}}
        }
    )

    create_meetings >> get_participants >> mid_merge >> get_raw_qos

    # https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html
    # https://airflow.apache.org/docs/apache-airflow/1.10.4/_api/airflow/operators/python_operator/index.html
    #value = task_instance.xcom_pull(task_ids='pushing_task') change id to 'create_meeting_df' for example, as that is the id
    #You need to push the result of one callable into another for this to function properly