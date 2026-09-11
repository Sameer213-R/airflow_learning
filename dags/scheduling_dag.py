# here we learining about the scheduling and backfilling of the data
# frist is scheduling is the process to scheduele the dag it follow the three thing 
# like frist one cron syntex than we delta scheduling 
# n cron we schedule the dag in the * * * * * pattren manner like min hous day(1,2) week , weekof day
# then we have the delta where we schedule the dag  explame in every 10 min alter 10 min like that
# catchup variable which take care of the backfilling of data
# suppose we have the data from 2016 and we deploy the dag in 2020 so whith the back fill or making 
#catchup vaiable true than we can backfill the ddata from 2016 to 2020

from airflow.sdk import task,dag
import pendulum
@dag(
        start_date= pendulum.datetime(year=2026,day=11,month=9,tz="Asia/Kolkata"),
        # scheduele = "@daily"  # this will run you dag from daily preset
        schedule= "38 11 * * *" , # this we have the cron pattern expression to set the thing 
        is_paused_upon_creation= False, # this will automatic staart the dag doing mannuly
             # if not mation than we have manully start the dag   
        catchup = False ,  # if we mation that catchup is flase it not backfill the data for that we have make it true                                                 
        end_date= pendulum.datetime(year=2026,month=10,day=21,tz="Asia/Kolkata") # this will end the dag on that date
)
def scheduling_dag():

    @task
    def task_1():

        print("this is task one ")
    
    @task.bash
    def task_2():
        return "echo 'this bash task '"

    task_1() >> task_2()

scheduling_dag = scheduling_dag()