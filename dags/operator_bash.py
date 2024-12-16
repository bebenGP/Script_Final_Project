from airflow.decorators import dag
from airflow.operators.bash import BashOperator

@dag()
def operator_bash_testing_aja():
    bash = BashOperator(
        task_id      = "bash",
        bash_command = "echo ini adalah operator bash",
    )

    bash

operator_bash_testing_aja()
