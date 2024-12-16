from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.email import send_email
from airflow.utils.context import Context
from airflow.utils.dates import days_ago

# Fungsi untuk mengirim email jika DAG berhasil
def send_email_on_success(context: Context):
    send_email(
        to=["bebengrahap@gmail.com"],
        subject="Airflow Success Notification!",
        html_content=f"""
            <center><h1>!!! DAG RUN FINAL PROJECT POTENTIAL MARKET SUCCESS !!!</h1></center>
            <b>DAG</b>: <i>{context['task_instance'].dag_id}</i><br>
            <b>Task</b>: <i>{context['task_instance'].task_id}</i><br>
            <b>Execution Date</b>: <i>{context['execution_date']}</i><br>
        """
    )

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={"on_success_callback": send_email_on_success},
    tags=["notification", "sensor"]
)
def monitor_and_notify_v2():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    trigger_final_project = TriggerDagRunOperator(
        task_id="trigger_final_project",
        trigger_dag_id="final_project_potential_market",
        wait_for_completion=False,  # Tidak menunggu langsung di sini
    )

    wait_for_dag = ExternalTaskSensor(
        task_id="wait_for_dag",
        external_dag_id="final_project_potential_market",
        external_task_id=None,     # Tunggu hingga seluruh DAG selesai
        poke_interval=30,          # Interval 30 detik
        timeout=1200,              # Timeout setelah 20 menit
        allowed_states=["success"],
        mode="poke",
    )

    start_task >> trigger_final_project >> wait_for_dag >> end_task

monitor_and_notify_v2()
