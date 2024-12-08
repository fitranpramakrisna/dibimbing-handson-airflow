from airflow.decorators import dag
from airflow.operators.bash import BashOperator

@dag()
def operator_bash():
    bash = BashOperator(
        task_id      = "bash",
        bash_command = "echo ini adalah operator bash",
    )

    # setiap kali membuat task, variable harus dipanggil, ini namanya control flow
    bash

operator_bash()