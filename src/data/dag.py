#1 get data catalog.csv, sevir and load gan model from gcp bucket
#2 predict data for given location and current timestamp
#3 save generated images in storage bucket

# Importing required modules#1 get data catalog.csv, sevir and load gan model from gcp bucket
#2 predict data for given location and current timestamp
#3 save generated images in storage bucket

# Importing required modules

from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from nowcast_api import nowcast

AWS_S3_CONN_ID = "AWS_Conn"
GCP_CONN_ID = "New_Goog_Conn"

# Defining default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": datetime(2019, 1, 1),
    "email": ["jaiswal.abh@northeastern.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@hourly",
}

def s3_connect():
	source_s3_key = "AKIA3VC3POILPCHWLUVT"
	source_s3_bucket = "sevir-bucket"
	dest_file = "/sevir"
	source_S3 = S3Hook(AWS_S3_CONN_ID)
	bucket_instance = source_S3.get_bucket(source_s3_bucket)
	
def gcp_fetch():
	lat=37.318363
	lon=-84.224203
	radius=100
	time_utc='2019-06-02 18:33:00'
	model_type = "gan"
	threshold_time_minutes = 30
	closest_radius = True
	force_refresh = False
	
	nowcast(lat, lon, radius, time_utc, model_type, threshold_time_minutes, closest_radius, force_refresh)
	
	
with DAG("s3_connect",start_date=datetime(2022,3,30),schedule_interval="@daily",catchup=False) as dag:
	#t1 = PythonOperator(task_id="s3_connect",python_callable=s3_connect)
	t2 = PythonOperator(task_id="gcp_connect",python_callable=gcp_fetch)
	t1 = GCSListObjectsOperator(task_id='GCS_Files',bucket='bda_assignment_sevir-s22',gcp_conn_id=GCP_CONN_ID)
	#t2 = GCSFileTransformOperator(task_id='GCS_transform',source_bucket='bda_assignment_sevir-s22',gcp_conn_id=GCP_CONN_ID)
	
t1 >> t2

# Instantiating the DAG
# with DAG("generate_nowcast_data", default_args=default_args) as dag:
#	Task_I = 
#	Task_II = 
#	Task_III = 
#	Task_IV =
#	Task_V = 
	
#Task_I >> Task_II >> Task_III >> Task_IV >> Task_V

	
#def connectGCS():
#	hook = GCSHook(gcp_conn_id='')
	


