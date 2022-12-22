from airflow import DAG
import random
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def branch_func(ti):
    xcom_value= int(ti.xcom_pull(key='k1'))
    if xcom_value >=5:
        print(f"value of xcom_value : {xcom_value}")
        return "continue_task"
    elif xcom_value <=4:
        print(f"value of xcom_value : {xcom_value}")
        return "stop_task"

def push_func(ti):
    ti.xcom_push(key="k1", value=random.randint(0,10))

dag = DAG('dependency_handling_dag_python',
          description='This dag is an example of handling dependency & xcom Variable',
          schedule_interval='@daily',
          start_date= datetime(2022,11,28),
          catchup= False,
          tags= ['example'],
          default_args={
              'depends_on_past':False,
              'retries':1,
              'retry_delay':timedelta(minutes=5)
          }
)
t1 = PythonOperator(task_id="t1", python_callable=push_func,dag=dag)

t2 = PythonOperator(task_id="t2", python_callable=branch_func,dag=dag)

t3 = BashOperator(task_id="continue_task", bash_command="echo continue_task")
t4 = BashOperator(task_id="stop_task", bash_command="echo stop_task; exit 1")

t1 >> t2 >> [t3, t4]