from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator as S3toR
import json
import requests
import os

#Load config file
with open("/home/ubuntu/airflow/config_api.json",'r') as config_file:
    api_host_key = json.load(config_file)
    
now = datetime.now()
dt_now_string = now.strftime("%d-%m-%Y-%H-%M")
s3_bucket_name = "csv-zillow-data-stg" #S3 bucket name for CSV data

def extract_zillow_data(**kwargs):
    url=kwargs['url']
    headers=kwargs['headers']
    querystring=kwargs['querystring']
    dt_string = kwargs['date_string']
    
    #RESPONSE
    response = requests.request("GET", url, headers=headers, params=querystring)
    response_data = response.json()
    
    #Specify the output file path
    output_dir = "/home/ubuntu/airflow/data"
    output_file_path = f"/home/ubuntu/airflow/data/zillow_data_{dt_string}.json"
    file_str = f'zillow_data_{dt_string}.csv'
    
    os.makedirs(output_dir, exist_ok=True)
    
    #Write the response data to a file
    with open(output_file_path, 'w') as output_file:
        json.dump(response_data, output_file, indent=4)
    output_list=[output_file_path, file_str]
    print("output_list[1]: ", output_list[1])
    return output_list


default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date': datetime(2023,1,31),
    'email': 'saahilshroff.1aug@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

with DAG('zillow_analytics_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    extract_zillow_data_var = PythonOperator(
        task_id='tsk_extract_zillow_data',
        python_callable=extract_zillow_data,
        op_kwargs={
            # TODO: https://rapidapi.com/ntd119/api/zillow-com4/playground/apiendpoint_dc40b6f9-1c7c-41b2-84fc-cef2f65f3c12 use this link to get the url
            'url':"https://zillow56.p.rapidapi.com/search",
            "headers":api_host_key,
            "querystring":{"location":"seattle, wa","output":"json","doz":"36m"},
            "date_string":dt_now_string
        }
    )
    
    load_to_s3 = BashOperator(
        task_id='tsk_load_to_s3',
        bash_command='aws s3 cp {{ ti.xcom_pull(task_ids="tsk_extract_zillow_data")[0] }} s3://zillow-rapid-api-data/'
    )
    
    is_csv_in_s3 = S3KeySensor(task_id='tsk_is_csv_in_s3',
                               bucket_key = '{{ti.xcom_pull(task_ids="tsk_extract_zillow_data")[1]}}',
                               bucket_name=s3_bucket_name,
                               aws_conn_id='aws_s3_conn',
                               wildcard_match=False,
                               timeout=600, #timeout for the sensor
                               poke_interval=60) #polling interval for the sensor
    
    transfer_csv_from_s3_to_redshift = S3toR(task_id='tsk_transfer_csv_from_s3_to_redshift',
                                             aws_conn_id='aws_s3_conn',
                                             redshift_conn_id='redshift_conn',
                                             schema='public',
                                             table='zillowData',
                                             s3_bucket=s3_bucket_name,
                                             s3_key='{{ti.xcom_pull(task_ids="tsk_extract_zillow_data")[1]}}',
                                             copy_options=["csv ignoreheader 1"],
                                            )
    
    extract_zillow_data_var >> load_to_s3 >> is_csv_in_s3 >> transfer_csv_from_s3_to_redshift
    