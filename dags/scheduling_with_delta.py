from airflow.sdk import task,dag
from airflow.timetables.trigger import DeltaTriggerTimetable
import pendulum
@dag(
        start_date=pendulum.datetime(year=2026,month=9,day=11,tz="Asia/Kolkata"),
        schedule=DeltaTriggerTimetable(delta=pendulum.duration(days=10)),
        is_paused_upon_creation=True
        
)
def delta_dag():

    @task
    def task_1():

        print("this is task one ")
    
    @task.bash
    def task_2():
        return "echo 'this bash task '"

    task_1() >> task_2()
delta_dag = delta_dag()