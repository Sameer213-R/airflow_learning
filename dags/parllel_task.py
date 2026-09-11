# here we learning about the parallel executing of task and how to build it

from airflow.sdk import task,dag

@dag
def parallel_task():
    
    @task.bash
    def open_task():
        return "echo 'loading data from s3 and SQL'"
    
    @task.python
    def task_sql():

        data = [1,2,3,4]
        return data
    
    @task.python
    def task_s3():
        data = [5,6]
        return data

    @task.python
    def collecting(ti):

        sql = ti.xcom_pull(task_ids="task_sql",key="return_value")
        s3 = ti.xcom_pull(task_ids="task_s3",key="return_value")

        processed = sql + s3

        print("here is the processed data ", processed)
    
    # creating paralle task 
    open_task() >> [task_s3(),task_sql()] >> collecting()

parallel_task = parallel_task()
