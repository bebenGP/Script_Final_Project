from airflow.decorators import dag
from airflow.operators.python import PythonOperator

def print_python():
    print("ini adalah operator python")

@dag()
def testing_airflow_onoff():
    python = PythonOperator(
        task_id         = "python",
        python_callable = print_python,
    )

    python

testing_airflow_onoff()



