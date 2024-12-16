from airflow.decorators import dag, task
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd 
import geopandas as gpd
import unicodedata
import sqlalchemy as sa
import folium
from folium.plugins import HeatMap
import numpy as np
from shapely.geometry import Point
import boto3
from io import StringIO
import json

@dag(
    dag_id            = "favorite_place_etl",
    description       = "Final Project",
    schedule_interval = "15 9-21/2 * * fri",
    start_date        = datetime(2024, 11, 1),
    catchup           = False,
    tags              = ["Final_Project"],
    default_args      = {
        "owner": "esri_indo, beben",
    },
    owner_links = {
        "esri_indo": "https://esriindonesia.co.id/id",
        "beben"    : "mailto:bebengrahap@gmail.com",
    }
)
def control_flow_decorator_complex_var():
    @task
    def python_1():
        print("task 1")

    @task
    def python_2():
        print("task 2")

    @task
    def python_3():
        print("task 3")

    @task
    def bash_2():
        print("task 4")

    @task
    def bash_3():
        print("task 5")
        
    @task
    def bash_1():
        print("task 6")

    task_python_1 = python_1()
    task_python_2 = python_2()
    task_python_3 = python_3()
    task_python_4 = bash_2()
    task_python_5 = bash_3()
    task_python_6 = bash_1()

    task_python_1 >> [task_python_3, task_python_2]
    [task_python_6, task_python_2] >> task_python_4
    [task_python_3, task_python_4] >> task_python_5

control_flow_decorator_complex_var()



