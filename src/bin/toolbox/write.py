"""
SUMMARY:
    Script that contain the set of abstractions created to write
    any dataset through the Machine Learning Operation Lifecycle.
"""
# !/usr/bin/env python
# -*- CODING: UTF-8 -*- VERSION: 3.6 -*-


# Neural Flow Framework Lib


# Python Libs



class Writer:
    """TODO"""

    def __init__(self, spark, config):
        self.spark  = spark
        self.config = config

    def df_to_parquet(self, df, path, w_mode=None):
        if w_mode:
            df.write.mode(w_mode).parquet(path)
        else:
            df.write.parquet(path)
        return

    def dict_to_df(self):
        """
        """
        pass

    def df_to_hive_table(self, df, table_config):
        df.write.saveAsTable(**table_config)

