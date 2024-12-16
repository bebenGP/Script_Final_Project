from airflow.decorators import dag, task
from airflow.models.dagrun import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.email import send_email
from airflow.utils.context import Context
from airflow.utils.dates import days_ago

# Fungsi untuk mengirim email jika DAG berhasil
def send_email_on_success(context: Context):
    send_email(
        to           = ["bebengrahap@gmail.com"],  # Email tujuan
        subject      = "Airflow Success Notification!",
        html_content = f"""
            <center><h1>!!! DAG RUN FINAL PROJECT POTENTIAL MARKET SUCCESS !!!</h1></center>
            <b>DAG</b>: <i>{ context['task_instance'].dag_id }</i><br>
            <b>Task</b>: <i>{ context['task_instance'].task_id }</i><br>
            <b>Execution Date</b>: <i>{ context['execution_date'] }</i><br>
        """
    )

@dag(
    schedule_interval=None,                # Tidak dijalankan secara periodik
    start_date=days_ago(1),                # Tanggal mulai backfill
    catchup=False,                         # Tidak melakukan backfill otomatis
    default_args={"on_success_callback": send_email_on_success},  # Callback pada keberhasilan
    tags=["notification", "sensor"]       # Tag untuk identifikasi DAG
)
def monitor_and_notify():
    # Task Placeholder untuk menandai awal dan akhir DAG
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    # Sensor untuk memonitor DAG final_project_potential_market
    wait_for_dag = ExternalTaskSensor(
        task_id="wait_for_dag",
        external_dag_id="final_project_potential_market",  # DAG ID yang dimonitor
        external_task_id=None,  # Memantau keseluruhan DAG, bukan task tertentu
        poke_interval=10,       # Interval pengecekan setiap 10 detik
        timeout=300,            # Timeout setelah 5 menit (300 detik)
        allowed_states=["success"],  # Memastikan status DAG eksternal harus "success"
        mode="poke",            # Sensor aktif secara sinkron
    )

    start_task >> wait_for_dag >> end_task

monitor_and_notify()
