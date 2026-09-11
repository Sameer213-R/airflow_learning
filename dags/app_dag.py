from airflow.sdk import task,dag

@dag
def app_dag():

    @task
    def task_1():
        print("this is dag")
    
    @task
    def task_2():
        print("this is 2 dag")
    
    task_1() >> task_2()

app_dag = app_dag()