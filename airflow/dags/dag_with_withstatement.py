from airflow import DAG
from airflow.operators.empty import EmptyOperator


# dengan with, kita ga perlu di assign kesuatu variable lagi, cukup di dalam blok with nya saja
with DAG(dag_id="dag_with_withstatement") as dag:
    task_1 = EmptyOperator(
        task_id = "task_ke_1",
    )

    task_1