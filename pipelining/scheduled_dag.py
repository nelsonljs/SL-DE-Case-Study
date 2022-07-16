from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os,sys;
from common.ETLRow import Pipeline1   # Add module to the dags, common folder for pythondag.

@dag(
    schedule_interval="0 0 * * *", # Schedule run once a day.
    start_date=datetime.today(),
    dagrun_timeout=timedelta(minutes=60),
)

def section1():
    @task
    def process_df_in_folder():

        myjob = Pipeline1()
        myjob.main()

    process_df_in_folder()

dag = section1()