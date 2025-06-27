# -*- coding: utf-8 -*-
import collections
import calendar
import datetime
import inspect
from dateutil.relativedelta import relativedelta
import os
import sys


class SparkConnection:
    """
    Connects to a Spark cluster and provides wrappers for commonly used Spark commands
    """
    def __init__(self, configs: dict):
        """
        Class constructor

        param spark_configs: 
		dictionary of configurations for pyspark.sql.SparkSession.config, where the
            keys are configuration property keys and values are configuration property values
            example - "spark.executor.memory": "30g"
        
	param spark_env_vars: 
		environment variables to set prior to creating a pyspark.sql.SparkSession instance
            example - "SPARK_HOME": "/opt/cloudera/parcels/CDH/lib/spark"
        
	param is_local: 
		bool to indicate whether the cluster should be local to the current file system or connect to a remote cluster
        """
        self.configs = configs if configs else {}
        self.app_name = self.configs.get("appName")
        self.spark_config = self.configs.get("spark_config")
        self.log_level = self.configs.get("logging_level")
        self._load_env()
        self._load_pyspark()
        self.session = self._create_session()

    def _load_env(self):
        #Updated to load dinamically pyspark and py4j zip files
        for f in os.listdir(os.environ["PYLIB"]) :
            if f.endswith('.zip'):
                sys.path.insert(0, os.path.join(os.environ["PYLIB"] , f ))
        return self

    def _load_pyspark(self):
        global pandas, pyspark, Window, udf, monotonically_increasing_id,lit, F
        # Third party libraries
        import pandas
        import pyspark
        #from pyspark.sql.types import *
        from pyspark.sql import Window
        from pyspark.sql.functions import udf
        from pyspark.sql.functions import monotonically_increasing_id
        from pyspark.sql.functions import lit
        import pyspark.sql.functions as F
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.stop()

    @property
    def application_id(self):
        """
        :returns: the Spark application id for this job/connection
        """

        return self.session.sparkContext.applicationId

    @property
    def hive(self):
        """
        returns: a HiveContext for this connection
        """
        hive = pyspark.HiveContext(self.session)
        return hive

    def sql(self, query):
        """ Execute SQL query
        Args:
            query ([String]): SQL statement
        Returns:
            [Pyspark Dataframe]: Result sql execution
        """
        df = self.session.sql(query)
        return df

    def table_exists(self, table_name):
        """
        Returns a bool whether the table (in format 'DB_NAME.TABLE_NAME') exists

        :param table_name: the name of the table
        :returns: True if the table exists, False otherwise
        """
        query = self.session._jsparkSession.catalog().tableExists(table_name)
        return query

    def cache_table(self, table_name):
        """
        Caches a table name in Spark
        """
        cache = self.session.catalog.cacheTable(table_name)
        return cache

    def uncache_table(self, table_name):
        """
        Uncaches a table name in Spark
        """
        uncache = self.session.catalog.uncacheTable(table_name)
        return uncache

    def temp_table(self, name, query):
        """
        Creates a temporary table using a Spark SQL query

        :param name: name of the temporary table to be created
        :param query: Spark SQL query to execute
        :returns: the new temporary table as a Spark DataFrame
        """
        table = self.sql(query)
        table.createOrReplaceTempView(name)
        return table

    def temp_table_from_dataframe(self, name, df):
        """
        Creates a temporary table using a DF Spark

        :param name: name of the temporary table to be created
        :param df: Spark DF
        :returns: the new temporary table as a Spark DataFrame
        """
        df.createOrReplaceTempView(name)
        return df

    def assert_table_not_empty(self, table_name):
        """
        Checks that a Spark SQL table is not empty

        :param table_name: the table to check
        """
        df = self.sql(f"SELECT * FROM {table_name}")
        if not df.rdd.isEmpty():
            return True
        return False


    def insert_table_from_tmp_table(self, target_table, input_data, partitions, type_insert):
        """
            Execute SQL insert using a temporary table as data input
            
	    Args:
               	
		target_table:    ([String]) Table name to insert
                
		input_data:      ([String]) Input table name
                
		partitions:      ([Dictionary]) Represents {"colum_name":"value"}
                
		type_insert:     ([String]) It could be Overrite or Into
            Returns:
                [Pyspark Dataframe]: Result sql execution
        """
        array_partitions = []
        for column, value in partitions.items():
            array_partitions.append(f"{colum} = '{value}'")

        partition = ",".join(array_partitions)

        df = self.sql(""" INSERT {type_insert}
                            TABLE {target_table}
                            PARTITION( {partition} )
                            SELECT * FROM  {input_data} """)
        return df


    def _addPyFile(self, path):
        self.session.sparkContext.addPyFile(path)

    def _create_session(self):
        """
        Open a Spark session using pyspark
        :returns: a pyspark.sql.SparkSession
        """

        # Set environment variables
        session_builder = pyspark.sql\
                            .SparkSession\
                            .builder.appName(self.app_name) \
                            .enableHiveSupport() \
                            .master("yarn")

        # Add configurations to builder
        for key, value in self.spark_config.items():
            session_builder = session_builder.config(key, value)

        session = session_builder.getOrCreate()
        session.sparkContext.setLogLevel(f"{self.log_level}")

        return session
