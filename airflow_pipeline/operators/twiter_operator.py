import sys 
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
import json
from hook.twiter_hook import TwiterHook
from datetime import datetime, timedelta
from os.path import join


class TwitterOperator(BaseOperator):

    def __init__(self, end_time, start_time, query, **kwargs):
        
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        super(). __init__(**kwargs)

    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query

        with open("extract_twitter.json", "w") as output_file:
            for pg in TwiterHook(end_time, start_time, query).run():
                json.dump(pg, output_file)
                output_file.write("\n")

if __name__ == "__main__":
   TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
   end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
   start_time = (datetime.now() + timedelta(-1)).strftime(TIMESTAMP_FORMAT)   
   query = "datascience"


with DAG (dag_id = "conectTeste", start_date=datetime.now()) as dag: 
    to = TwitterOperator(query=query, start_time=start_time, end_time=end_time, task_id="teste_run")
    
    ti = TaskInstance(task=to)
    to.execute(ti.task_id)