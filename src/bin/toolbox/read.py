"""
SUMMARY:
    Script that contain the set of abstractions created to read any file
    needed within the Machine Learning Operation Lifecycle.
"""
# !/usr/bin/env python
# -*- CODING: UTF-8 -*- VERSION: 3.6 -*-

# Neural Flow Framework Lib
from lib_log import LogHandler as Logger
# Python Libs
from dataclasses import dataclass
import json

@dataclass
class Reader:
    path: str

    def read_json(self):
        """TODO"""
        data = dict()
        Log = Logger()

        try:
            with open(self.path) as json_file:
                data = json.load(json_file)
            Log.log_info(f"JSON read successfully: \n\t{str(self.path)}")
            return data
        except Exception as e:
            Log.log_error()
            raise e

class InReader(Reader):
    """TODO: in-side reader stuff depending of the spark session"""

    def __init__(self, path="", spark=None, config=None):
        super().__init__(path)
        self.spark  = spark
        self.config = config

    def parquet_to_df(self,path_=""):
        """Reads the path_parquet into a Pyspark DataFrame."""
        Log = Logger()
        try:
            #df = self.spark.read.parquet(self.path)
            df = self.spark.read.parquet(path_)
            Log.log_info(f"Parquet file read successfully: \n\t{str(path_)}")
            Log.log_info()
            return df
        except Exception as e:
            Log.log_error()
            raise e

    def hive_table_to_df(self,schema,tablename):
        """Reads the Hive table into a Pyspark DataFrame."""
        Log = Logger()
        try:
            df = self.spark.table(f"{schema}.{tablename}")
            Log.log_info(f"Hive table read successfully: \n\t{'{schema}.{tablename}'} ")
            Log.log_info()
            return df
        except Exception as e:
            Log.log_error()
            raise e

