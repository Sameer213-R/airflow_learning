# here we building the dag with the condition
from airflow.sdk import task,dag


@dag
def conditional_dag():

    @task.bash
    def bash_task():
        return "echo 'this tak is bashed' "

    @task.python
    def sql_data():
        data = [1,2,3,4,4]

        print("i have this data :" , data)
        return data
    
    @task.python
    def file_data():
        data = [5,6,7,8]

        print("i have this data :" ,data)
        return data

    @task.python
    def proccssing(ti):

        file_data = ti.xcom_pull(task_ids="file_data",key="return_value")
        sql_data = ti.xcom_pull(task_ids="sql_data", key="return_value")

        process_data = sql_data + file_data
        return process_data
    
    @task.branch
    def task_branch(ti):

        process_data = ti.xcom_pull(task_ids="proccssing",key="return_value")

        if len(process_data) > 5:
            return 'sql_load'
        else:
            return 's3_load'
    

    @task.python
    def sql_load(ti):
        data = ti.xcom_pull(task_ids="proccssing",key="return_value")
        print("loading data ... to sql",data)
    
    @task.python
    def s3_load(ti):
        data = ti.xcom_pull(task_ids="proccssing",key="return_value")
        print("laoding data ... to s3",data)
    

    #building the graph flow
    bash_task() >> [file_data(),sql_data()] >> proccssing() >> task_branch() >> [sql_load(),s3_load()]

conditional_dag = conditional_dag()