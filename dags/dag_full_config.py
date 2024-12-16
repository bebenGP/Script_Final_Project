from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    dag_id            = "dag_full_config",
    description       = "ini adalah deskripsi dag",
    schedule_interval = "* * * * *",
    start_date        = datetime(2024, 7, 1),
    catchup           = False,
    tags              = ["exercise"],
    default_args      = {
        "owner": "esri_indo, beben",
    },
    owner_links = {
        "esri_indo": "https://esriindonesia.co.id/id",
        "beben"    : "mailto:bebengrahap@gmail.com",
    }
)
def main():
    task_1 = EmptyOperator(
        task_id = "task_ke_1"
    )

    task_1

main()

