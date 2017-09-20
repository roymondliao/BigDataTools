#!/usr/bin/env python3.5

import json
import airflow
import requests
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.models import TaskInstance

# Variable setting
VARIABLE_CONF = Variable.get('slack')
SLACK_CONF = json.loads(VARIABLE_CONF)
SLACK_CHANNEL = SLACK_CONF['channel']
SLACK_TOKEN = SLACK_CONF['token']
SLACK_NAME = SLACK_CONF['name']
SLACK_HOOK = SLACK_CONF['hook']

# DAG args
args = {
    'owner': 'yuyuliao',
    'depends_on_past': False,
    'start_date': datetime(2017, 9, 20, 0, 0),
    'email': ['yuyuliao@onead.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'provide_context': True, # Define function argument : **kwargs, it could use the xcom to push and pull the values
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(seconds=10)
}

def sla_alert_func(dag, task_list, blocking_task_list, slas, blocking_tis):
    message = "{dag_name} SLA Miss for task `{task_list}`".format(**{
        'dag_name': dag.dag_id,
        'task_list': task_list})
    data = {'text': message,
            'channel': SLACK_CHANNEL,
            'username': SLACK_NAME}
    requests.post(url=SLACK_HOOK, data=json.dumps(data))

dag = DAG('slack_testing', description='Test slack post api',
          default_args=args,
          schedule_interval='10 * * * *', 
          catchup=False,
          sla_miss_callback=sla_alert_func)

def push_value(**kwargs):
    # kwargs can get the all information of DAG
    kwargs['ti'].xcom_push(key='Random number', value=np.random.choice(range(0, 100)))
     

def pull_value(**kwargs):
    ti = kwargs['ti']
    value = ti.xcom_pull(task_ids='push', key='Random number', dag_id='slack_testing')
    return 'Get Value:{0}, Compute value:{1}'.format(value, value*2)


slack = SlackAPIPostOperator(task_id="slack_post", 
                             token=SLACK_TOKEN,
                             channel=SLACK_CHANNEL,
                             username=SLACK_NAME,
                             text=":red_circle: DAG Failed", 
                             dag=dag)

push = PythonOperator(task_id='push', python_callable=push_value, dag=dag)

pull = PythonOperator(task_id='pull', python_callable=pull_value, dag=dag)

push.set_downstream(pull)
pull.set_downstream(slack)
