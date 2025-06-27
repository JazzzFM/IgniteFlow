# -*- coding: utf-8 -*-
import argparse

def parse_arguments():
    """
    Parse arguments provided by spark_exe script

    Args:
	ENVIRONMENT: ([String]) Name of the environment which will be excecuted.

	MODEL: ([String]) Name of the model that will be excecuted.

	BOX: ([String]) Name or alias for the box of that MODEL.

	JOB: ([String]) Name of the script of an independent data proces/pipeline.

    Returns:
        [Object]: Object that contains each parameter as atribute.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-env", "--environment")
    parser.add_argument("-mod", "--model_name")
    parser.add_argument("-b", "--box_name")
    parser.add_argument("-j", "--job_name")
    return parser.parse_args()
