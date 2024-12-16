from airflow.decorators import dag
from airflow.operators.bash import BashOperator

@dag()
def beben_testing_airflow():
    bash = BashOperator(
        task_id      = "bash",
        bash_command = "echo ini adalah operator bash",
    )

    bash

beben_testing_airflow()
