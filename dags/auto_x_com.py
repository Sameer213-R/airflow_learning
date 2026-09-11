#  here we send the data from one to other with auto method 
# with using the push manual we return the data 
from airflow.sdk import task ,dag

@dag
def auto_x_com():

    @task
    def fetching_data():
        data = {"name":"airflow","version":3.0}
        return data
    @task
    def revice_data(ti):
        data = ti.xcom_pull(task_ids="fetching_data",key="return_value")
        print("pull data ", data)
    
    fetching_data() >> revice_data()

auto_x_com = auto_x_com()

