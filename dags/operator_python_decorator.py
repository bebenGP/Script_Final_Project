from airflow.decorators import dag, task

@dag()
def operator_python_decorator():
   @task
   def python_1(param1, **kwargs):
       print("ini adalah operator python dengan decorator")
       print(param1)
       print(kwargs['param2'])
  
   @task(task_id="declarative_task_id")
   def python_2(param1, **kwargs):
       print("ini adalah operator python dengan decorator")
       print(param1)
       print(kwargs['param2'])

   python_1(
       param1 = "ini adalah param1",
       param2 = "ini adalah param2",
   )

   python_2(
       param1 = "ini adalah param1",
       param2 = "ini adalah param2",
   )

operator_python_decorator()



