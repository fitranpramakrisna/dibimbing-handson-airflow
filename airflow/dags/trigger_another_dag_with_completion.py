from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag()
def trigger_another_dag_with_completion():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    trigger_dag_sleep = TriggerDagRunOperator(
        task_id             = "trigger_dag_sleep",
        trigger_dag_id      = "sleep_python",
        wait_for_completion = True,
        poke_interval       = 5, # cara mengetahui apakah si trigger yg lain udah selesai apa blm
        # setiap 5 detik, operator ini apakah trigger lain sudah selesai apa blm
    )

    start_task >> trigger_dag_sleep >> end_task

trigger_another_dag_with_completion()