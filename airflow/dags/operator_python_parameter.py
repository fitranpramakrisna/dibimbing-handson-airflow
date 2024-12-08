from airflow.decorators import dag
from airflow.operators.python import PythonOperator

# pake parameter
def print_python(param1, **kwargs):
    print("ini adalah operator python")
    print(param1)
    print(kwargs['param2'])

@dag()
def operator_python_parameter():
    python = PythonOperator(
        task_id         = "python",
        python_callable = print_python,
        # ini cara isi parameter fungsi print_python nya
        op_kwargs       = {
            "param1": "ini adalah param1",
            "param2": "ini adalah param2",
        },
    )

    python

operator_python_parameter()

