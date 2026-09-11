from airflow.sdk import task,dag
import pendulum
from airflow.timetables.events import EventsTimetable

event_oject = EventsTimetable(
event_dates=[pendulum.datetime(2026,10,21)]
)

@dag(
       start_date= pendulum.datetime(year=2026,month=10,day=1,tz="Asia/Kolkata"),
       schedule= event_oject
        
)
def special_event_dag():

    @task
    def task_1():

        print("this is task one ")
    
    @task.bash
    def task_2():
        return "echo 'this bash task '"

    task_1() >> task_2()
special_event_dag = special_event_dag()