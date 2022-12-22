from airflow import DAG
import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def testfunc():
    print("test function executed")

with DAG(
    "composer_bwt_session",
    description = "This is composer DAG created in BWT session",
    start_date = datetime.datetime(2022,11,24),
    schedule_interval = datetime.timedelta (days=1),
    catchup= False,
    default_args = {
        'retries':1,
        'retry_delay' : datetime.timedelta(minutes=5),
        'depends_on_past' : False
    }
) as dag:

    t1 = BashOperator(
        task_id="first_bash_task",
        bash_command='date'
    )

    t2 = BashOperator(
        task_id="second_bash_task",
        bash_command='pwd'
    )

    t3 = BashOperator(
        task_id= "Third_bash_task",
        bash_command= "python /home/airflow/gcs/dags/simple-exp.py"
    )

    t4 = PythonOperator(
        task_id= "Fourth_Python_task",
        python_callable= testfunc
    )

    t1 << t2 << t3 << t4