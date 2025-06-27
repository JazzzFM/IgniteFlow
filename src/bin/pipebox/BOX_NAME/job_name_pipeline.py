from bnmxverse.dapull.pivote_auxiliar import collect_history_pivot
from typing import NoReturn

def BOX_NAME_JOB_NAME_collect(spark: object, config: object) -> NoReturn:
    """
        This  is the use of all tools from bin/bnnmxverse/ and bin/toolbox/
        that can be used for te train a model and generate data for a model.

        Args:
            spark    ([String]): This object contains SparkSession and SparkContext and SparkSql
            config   ([String]): This object contains all configuration in any json config/

        Returns:
            [None]: Result of data tratement pipeline
    """
    jstable  = config.tables.get("table_a").get("table_name")
    print(f"jstable: {jstable} \n")
