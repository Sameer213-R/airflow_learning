from airflow.sdk import task,dag
from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum
@dag(
        start_date= pendulum.datetime(year=2026,month=9,day=2,tz="Asia/Kolkata"),
        schedule = CronDataIntervalTimetable('0 0 * * *',timezone="Asia/Kolkata"),
        catchup=True,
        is_paused_upon_creation=False
)
def incremental_load():

    @task.python
    def extract_data(**kwargs):

        # pick the from date 
        from_date = kwargs.get('data_interval_start')
        end_date = kwargs.get('data_interval_end')

        print(f"extracting data from {from_date} to {end_date}")

        print(f"""
                    select * from orders where 
                    order_date >= {from_date}
                    and
                    order_date < {end_date}
        """)
    
    @task.bash
    def bash_task():
        return "echo 'this bash task for reading file'"


    extract_data() >> bash_task()

incremental_load = incremental_load()
