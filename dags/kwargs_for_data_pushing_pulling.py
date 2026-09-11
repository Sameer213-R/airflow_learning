from airflow.sdk import task ,dag

@dag
def kwargs_data():

    @task
    def task_1(**kwargs):
        #printing the kwargs attributes
        print("attributes of kwargs ",kwargs)

        # taking the ti attribute form kwargs
        ti = kwargs['ti']

        data = [1,23,4,4]
        ti.xcom_push(key="task_1",value=data)

    @task
    def task_2(**kwargs):
        ti = kwargs['ti']

        data=ti.xcom_pull(task_ids="task_1",key="task_1")
        print("print what we recived ",data)
    
    task_1() >> task_2()

kwargs_data = kwargs_data()