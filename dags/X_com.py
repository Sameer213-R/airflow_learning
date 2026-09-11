#here we building the x_com which help to transfer the data between the two task 
# here is have push and pull meathod to do this thing 
# and here ti mean task instance which help to pull or push the data
from airflow.sdk import task ,dag

@dag
def x_com():

    @task
    def fetching_data(ti):
        data = {"name":"airflow","version":3.0}
        ti.xcom_push(key="fetching_data",value=data)
        print("sending data",data)

    @task
    def revice_data(ti):
        data = ti.xcom_pull(task_ids="fetching_data",key="fetching_data")
        print("pull data ", data)
    
    fetching_data() >> revice_data()

x_com = x_com()

