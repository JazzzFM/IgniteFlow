# -*- coding: utf-8 -*-
from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable

def melt(df: DataFrame, id_vars: Iterable[str], value_vars: Iterable[str], \
    var_name: str = "variable", value_name: str = "value"):
    _vars_and_vals = array(*(struct(lit(c).alias(var_name), col(c).alias(value_name)) for c in value_vars))
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))
    cols = id_vars + [col("_vars_and_vals")[x].alias(x) for x in [var_name,value_name]]
    return _tmp.select(*cols)
