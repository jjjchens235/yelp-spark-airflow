from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

from airflow.hooks.S3_hook import S3Hook
from airflow import settings
from airflow.models import Connection

from datetime import datetime
import json
from sqlalchemy.orm import exc

with open('./dags/aws_credentials.json') as f:
    credentials = json.load(f)

#the s3 bucket where the yelp data set is saved
BUCKET_NAME = 'spark-yelp'
local_data_path_prefix = './dags/'
local_data_path_main = 'yelp_dataset/yelp_academic_dataset_'

local_review_file = local_data_path_prefix + local_data_path_main + 'review.json'
local_business_file = local_data_path_prefix + local_data_path_main + 'business.json'
s3_review_file = local_data_path_main + 'review.json'
s3_business_file = local_data_path_main + 'business.json'

local_script = './dags/scripts/yelp_script.py'
s3_script = 'scripts/yelp_script.py'
s3_output = 'outputs/'


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    'wait_for_downstream': True,
    "start_date": datetime(2021, 4, 3),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}


dag = DAG("spark_submit", default_args=default_args,
          schedule_interval="0 9 * * *", max_active_runs=1)


def update_aws_default(conn_id, login, password, extra):
    """ Update aws_default connection with login and pw"""
    session = settings.Session()
    try:
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).one()
    except exc.NoResultFound:
        conn = None
    if conn:
        conn.login = login
        conn.password = password
        conn.extra = extra

        session.add(conn)
        session.commit()


def local_files_to_s3(filenames, keys, bucket_name=BUCKET_NAME, is_replace=False):
    """
    Move local data and scripts to s3
    """
    if len(keys) != len(filenames):
        raise ValueError('The number of local files needs to match the number of s3 keys provided')
    s3 = S3Hook()
    for i in range(len(filenames)):
        try:
            s3.load_file(filename=filenames[i], bucket_name=bucket_name, replace=is_replace, key=keys[i])
        except ValueError as e:
            print(f'\nfilenames: {filenames}\n error message was: {e}')


create_aws_conn = PythonOperator(
    dag=dag,
    task_id='create_aws_conn',
    python_callable=update_aws_default,
    op_kwargs={'conn_id': 'aws_default', 'login': credentials['login'], 'password': credentials['password'], 'extra': json.dumps(dict(region_name="us-west-1"))}
)

data_to_s3 = PythonOperator(
    dag=dag,
    task_id='data_to_s3',
    python_callable=local_files_to_s3,
    op_kwargs={'filenames': [local_review_file, local_business_file], 'keys': [s3_review_file, s3_business_file]}
)

script_to_s3 = PythonOperator(
    dag=dag,
    task_id='script_to_s3',
    python_callable=local_files_to_s3,
    op_kwargs={'filenames': [local_script], 'keys': [s3_script], 'is_replace': True}
)


JOB_FLOW_OVERRIDES = {
    "Name": "Yelp Dataset",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "Ec2KeyName": "de",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag
)


SPARK_STEPS = [
{
"Name": "Move all raw yelp data from S3 to HDFS",
"ActionOnFailure": "CANCEL_AND_WAIT",
"HadoopJarStep": {
    "Jar": "command-runner.jar",
    "Args": [
        "s3-dist-cp",
        "--src=s3://{{ params.BUCKET_NAME }}/yelp_dataset",
        "--dest=/yelp_dataset",
    ],
},
},
{
    "Name": "Classify yelp reviews",
    "ActionOnFailure": "CANCEL_AND_WAIT",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--deploy-mode",
            "client",
            "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
        ],
    },
},
{
    "Name": "Move output data from HDFS to S3",
    "ActionOnFailure": "CANCEL_AND_WAIT",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "s3-dist-cp",
            "--src=/output",
            "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_output }}",
        ],
    },
},
]


# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_review_file": s3_review_file,
        "s3_business_file": s3_business_file,
        "s3_script": s3_script,
        "s3_output": s3_output,
    },
    dag=dag
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[" +
    str(last_step) +
    "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

#create emr cluster first, then move script because that way you can edit the script if need be without restarting the cluster
create_aws_conn >> create_emr_cluster >> [data_to_s3, script_to_s3] >> step_adder >> step_checker >> terminate_emr_cluster
