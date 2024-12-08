from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

@dag()
def control_flow_parallel():
    task_1 = EmptyOperator(task_id="task_1")
    task_2 = EmptyOperator(task_id="task_2")
    task_3 = EmptyOperator(task_id="task_3")
    task_4 = EmptyOperator(task_id="task_4")
   
    # task 2 dan 3 akan dijalankan secara paralel (secara bersamaan)
    # task 4 akan dijalankan jika task 2 da 3 selesai
    task_1 >> [task_2, task_3] >> task_4

control_flow_parallel()