# -*- coding: utf-8 -*-
from pipebox.BOX_NAME.job_name_pipeline import BOX_NAME_JOB_NAME_collect
from typing import NoReturn
import traceback 


def run_job(conection: object, config: object) -> NoReturn:
    """
        This  is the use of all tools from bin/pipebox/BOX/
        that can be used for excecute all functions that treat data for a model.

        Args:
            connection  ([String]): This object contains SparkSession and SparkContext and SparkSql
            config      ([String]): This object contains all configuration in any json config/

        Returns:
            [None]: Result of data tratement of job pipeline
    """
    try:
        print("PROCESS STARTS")

        BOX_NAME_JOB_NAME_collect(
            spark = conection.session,
            config = config)

        print("ENDS ---------")
    except:
        traceback_output = traceback.format_exc()
        print(traceback_output)
