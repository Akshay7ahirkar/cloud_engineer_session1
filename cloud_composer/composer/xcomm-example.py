from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

def branch_func(ti):
    xcom_value= int(ti.xcom_pull(key='k1'))
    if xcom_value >=5:
        return "continue_task"
    elif xcom_value >=3:
        return "stop_task"
    else:
        return None

dag = DAG('dependency_handling_dag',
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
t1 = BashOperator(task_id="t1",
                  bash_command='echo "{{ ti.xcom_push(key="k1",value="v1") }}" "{{ti.xcom_push(key="k2",value="v2") }}"',dag=dag)


t2 = BashOperator(task_id="t2",
                  bash_command='echo "{{ ti.xcom_pull(key="k1") }}" "{{ ti.xcom_pull(key="k2") }}"',dag=dag)


t1 >> t2