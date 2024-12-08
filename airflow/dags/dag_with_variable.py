from airflow import DAG
from airflow.operators.empty import EmptyOperator

# panggil kelas dag
# menggunakan variable = masukin ke objek dag

#nama dag_with_variable itu hanya terpengaruh ke dag_id, bukan nama file
# jadi kalo nama filenya a, ya nama dag nya bukan a, tergantung di dag id
# kalo dag_id = x, ya nama dag nya nanti x
# best practice = nama file dan dag nya harus disamakan
dag = DAG(dag_id="dag_with_variable")

# emptyoperator = task yg ga ngapa2in
task_1 = EmptyOperator(
    task_id = "task_ke_1",
    dag     = dag,
)

task_1
