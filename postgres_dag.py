import psycopg2
import os
import pandas as pd
import datetime
from airflow.decorators import task, dag, task_group
from airflow.providers.mysql.hooks.mysql import MySqlHook

default_args = {
    'owner': 'Long_Nguyen',
}

@dag(dag_id="etl_local_to_postgres",
     default_args=default_args,
     schedule=None,
     start_date=datetime.datetime(2021, 12, 1))
def taskflow():
    @task
    def get_files():
        files_list = []
        for file in os.listdir("./dags/dags/Personal_projects/Postgres/Data/"):
            files_list.append(file)
        return files_list
    
    @task
    def extract(file_name):
        dataframe = pd.read_csv("./dags/dags/Personal_projects/Postgres/Data/{}".format(file_name))
        return dataframe
    
    @task
    def transform(dataframe):
        dataframe['Gender'] = dataframe['Gender'].astype('string')
        dataframe['Education Level'] = dataframe['Education Level'].astype('string')
        dataframe['Job Title'] = dataframe['Job Title'].astype('string')
        dataframe['Education Level'].fillna(dataframe['Education Level'].mode().iloc[0],inplace=True)
        dataframe['Years of Experience'].fillna(dataframe['Years of Experience'].mean(),inplace=True)
        dataframe['Salary'].fillna(dataframe['Salary'].mean(),inplace=True)
        dataframe.dropna(subset=['Age', 'Gender','Job Title'], inplace=True)
        return dataframe
    
    @task
    def load(dataframe):
        hook = MySqlHook(mysql_conn_id='mysql_conn_externaldb', schema='test')
        hook.insert_rows(table='salary', rows=dataframe.values.tolist())

    @task_group
    def etl(file_name):
        extract_file = extract(file_name)
        transform_file = transform(extract_file)
        load_file = load(transform_file)

    files_list = get_files()
    etl_process = etl.expand(file_name=files_list)
    
taskflow()