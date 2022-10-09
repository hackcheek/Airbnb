import os
import json
import src.api as api
import datetime as dt

from pyspark.sql import SparkSession
from src import spark_session
from typing import Dict
from typing import List
from typing import Tuple


class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode('utf-8')
        return json.JSONEncoder.default(self, obj)


class Dump:
    
    def __init__(self, renew=False):
        self.logpath = 'datasets/logs/'
        self.renew = renew
    

    def default_request(self) -> Tuple[List[str], JSON]:
        url: str = api.url
        params: Dict = api.params
        requester = api.AsyncRequest(url, params=params)
        error, content = requester.run()
        print("ERRORS:", error)
        return error, content


    def fetch(self):
        if len(os.listdir(self.logpath)) == 0 or self.renew:
            _, content = self.default_request()

        else:
            max_file = max(
                dt.datetime.strptime(
                    date.removesuffix('.json'), '%d-%m-%y'
                )
                for date in 
                os.listdir('datasets/logs')
            ).strftime('%d-%m-%y') + '.json'
            path = os.path.join(self.logpath, max_file)
            with open(path, 'r') as file:
                content = json.load(file)

        return content


    def to_json(self):
        content = self.fetch()
        today_date = dt.date.today().strftime("%d-%m-%y")
        json_content = json.dumps(content, cls=BytesEncoder)
        path = os.path.join(self.logpath, f'{today_date}.json')
        with open(path, 'w') as file:
            file.write(json_content)
    

    def to_delta(self):
        spark: SparkSession = spark_session()
        data = spark.read.json(content)
        fields = data.select('features').rdd.flatMap(lambda x: x).toDF()

