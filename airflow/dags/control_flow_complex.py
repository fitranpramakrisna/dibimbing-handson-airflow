from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

@dag()
def control_flow_complex():
    task_1 = EmptyOperator(task_id="task_1")
    task_2 = EmptyOperator(task_id="task_2")
    task_3 = EmptyOperator(task_id="task_3")
    task_4 = EmptyOperator(task_id="task_4")
    task_5 = EmptyOperator(task_id="task_5")
    task_6 = EmptyOperator(task_id="task_6")
    task_7 = EmptyOperator(task_id="task_7")
    task_8 = EmptyOperator(task_id="task_8")
    task_9 = EmptyOperator(task_id="task_9")
   
    # di control flow, task dipanggil berulang gpp
    # yg tidak ada dependensi nya, akan dijalankan duluan contoh task 1 dan 5
    task_1 >> [task_2, task_3] >> task_4 >> [task_6, task_7]
    task_5 >> [task_7, task_8] >> task_9

control_flow_complex()