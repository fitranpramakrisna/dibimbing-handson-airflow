from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

@dag()
def control_flow_sequence():
    task_1 = EmptyOperator(task_id="task_1")
    task_2 = EmptyOperator(task_id="task_2")
   
    # task 2 akan dijalankan setelah task 1 selesai
    task_1 >> task_2

control_flow_sequence()