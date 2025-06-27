# -*- coding: utf-8 -*-
from pyspark.sql.window import Window
from typing import Dict, List, NoReturn
from dataclasses import dataclass, field
import pyspark.sql.functions as F
from pyspark.sql.functions import when
from toolbox.melt import melt
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import DataFrame
import logging
from datetime import datetime
import itertools
@dataclass
class BigtableTransformer:

    @classmethod
    def __init__(self: object, spark: object, dict_attrs: Dict[str, str]) -> List:
        """
        This method allows you to set the atributes to this class,
        and verify if it contains all necesary to execute.
        """
        self.dict_attrs = dict_attrs
        self.spark = spark
        self.last_updated_date = datetime.today().strftime("%Y-%m-%d")

        self.attrs = ["model_id",
                        "date_format",
                        "model_score_table",
                        "partition",
                        "cust_id",
                        "decile",
                        "info_date",
                        "model_type",
                        "type_scores",
                        "measure_name",
                        "score_model_name"]

        for k, v in self.dict_attrs.items():
            setattr(self, k, v)

        for k in self.attrs:
            if k not in self.dict_attrs.keys():
                raise Exception(f"The key {k} must exists in your configuration file.")

        self.not_scores = ["model_id",
                            "last_updated_date"
                            "score_process_date",
                            "customer_id",
                            "model_type_execution"]

    @classmethod
    def query_score(self: object) -> DataFrame:
        # This query has to be the fisrt Step
        self.query = self.spark.sql(f""" SELECT *
                        FROM {self.model_score_table}
                        WHERE {self.partition} = (
                            SELECT MAX({self.partition})
                        FROM {self.model_score_table}) """)\
                    .withColumnRenamed(f"{self.cust_id}", "customer_id")\
                    .withColumnRenamed(f"{self.partition}", "score_process_dt")\
                    .withColumnRenamed(f"{self.info_date}", "score_info_dt")\
                    .withColumn("score_process_dt",
                        F.from_unixtime(
                            F.unix_timestamp(F.col('score_process_dt'), 'yyyy-MM-dd'),
                            "yyyy-MM-dd HH:mm:ss").cast("String"))\
                    .withColumn("score_info_dt",
                        F.from_unixtime(
                            F.unix_timestamp(F.col('score_info_dt'), 'yyyy-MM-dd'),
                            "yyyy-MM-dd HH:mm:ss").cast("String"))\
                    .withColumn("score_process_dt",
                        F.regexp_replace(F.col("score_process_dt"), ' ', 'T'))\
                    .withColumn("score_process_dt",
                        F.concat(F.col("score_process_dt"), F.lit("Z")))\
                    .withColumn("score_info_dt",
                        F.regexp_replace(F.col("score_info_dt"), ' ', 'T'))\
                    .withColumn("score_info_dt",
                        F.concat(F.col("score_info_dt"), F.lit("Z")))\
                    .withColumn("model_type_execution", F.lit(int(self.model_type)))\
                    .withColumn("score_model_name", F.lit(f"{self.score_model_name}"))\
                    .withColumn("$date", F.col("score_info_dt"))\
                    .withColumn("credit_approved_dt", F.to_json(F.struct(F.col("$date"))))\
                    .drop("$date")\
                    .withColumn("$date", F.col("score_process_dt"))\
                    .withColumn("score_process_dt", F.to_json(F.struct(F.col("$date"))))\
                    .drop("$date")\
                    .withColumn("$date", F.col("score_info_dt"))\
                    .withColumn("score_info_dt", F.to_json(F.struct(F.col("$date"))))\
                    .drop("$date")\
                    .withColumn("measure_name", F.lit(f"{self.measure_name}"))\
                    .withColumn("credit_approved_flag", F.lit("true"))\
                    .withColumn("last_updated_date",
                            F.from_unixtime(
                                F.unix_timestamp(F.lit(f"{self.last_updated_date}"), 'yyyy-MM-dd'),
                                "yyyy-MM-dd HH:mm:ss").cast("String"))\
                    .withColumn("last_updated_date",
                        F.regexp_replace(F.col("last_updated_date"), ' ', 'T'))\
                    .withColumn("last_updated_date",
                        F.concat(F.col("last_updated_date"), F.lit("Z")))

        self.score_cols = [cul for cul, typ in self.query.dtypes\
                                if cul not in self.not_scores\
                                and typ == f"{self.type_scores}"]

        self.last_updated = self.query.select("last_updated_date").first()[0]
        print(f"{self.last_updated}")
        # print(df["score_info_dt"][0])
        # print(df["credit_approved_dt"][0])
        return self.query

    @classmethod
    def transform_measure(self: object) -> DataFrame:

        if self.decile == "False":

            def asign_bins(df, score_col, bins_min, bins_max):
                if len(bins_min) == len(bins_max):

                    df = df.withColumn(f"decile_{score_col}",
                                when((F.col(f"{score_col}") >= bins_min[0]) & (F.col(f"{score_col}") <= bins_max[0]), F.lit(1).cast("Integer"))\
                                .when((F.col(f"{score_col}") >= bins_min[1]) & (F.col(f"{score_col}") <= bins_max[1]), F.lit(2).cast("Integer"))\
                                .when((F.col(f"{score_col}") >= bins_min[2]) & (F.col(f"{score_col}") <= bins_max[2]), F.lit(3).cast("Integer"))\
                                .when((F.col(f"{score_col}") >= bins_min[3]) & (F.col(f"{score_col}") <= bins_max[3]), F.lit(4).cast("Integer"))\
                                .when((F.col(f"{score_col}") >= bins_min[4]) & (F.col(f"{score_col}") <= bins_max[4]), F.lit(5).cast("Integer"))\
                                .when((F.col(f"{score_col}") >= bins_min[5]) & (F.col(f"{score_col}") <= bins_max[5]), F.lit(6).cast("Integer"))\
                                .when((F.col(f"{score_col}") >= bins_min[6]) & (F.col(f"{score_col}") <= bins_max[6]), F.lit(7).cast("Integer"))\
                                .when((F.col(f"{score_col}") >= bins_min[7]) & (F.col(f"{score_col}") <= bins_max[7]), F.lit(8).cast("Integer"))\
                                .when((F.col(f"{score_col}") >= bins_min[8]) & (F.col(f"{score_col}") <= bins_max[8]), F.lit(9).cast("Integer"))\
                                .when((F.col(f"{score_col}") >= bins_min[9]) & (F.col(f"{score_col}") <= bins_max[9]), F.lit(10).cast("Integer"))\
                                .otherwise(F.lit(0).cast("Integer")))\
                                .withColumn(f"quartile_{score_col}",
                                    when((F.col(f"{score_col}") >= bins_min[0]) & (F.col(f"{score_col}") <= bins_max[1]), F.lit(1).cast("Integer"))\
                                    .when((F.col(f"{score_col}") >= bins_min[2]) & (F.col(f"{score_col}") <= bins_max[4]), F.lit(2).cast("Integer"))\
                                    .when((F.col(f"{score_col}") >= bins_min[5]) & (F.col(f"{score_col}") <= bins_max[7]), F.lit(3).cast("Integer"))\
                                    .when((F.col(f"{score_col}") >= bins_min[8]) & (F.col(f"{score_col}") <= bins_max[9]), F.lit(4).cast("Integer"))\
                                    .otherwise(F.lit(0).cast("Integer")))\

                    return df
                else:
                    raise("""Error in your copy_bts.json config file,
                            list of bins must have the same length.""")

            bins_min = list(map(float, self.bins_min))
            bins_max = list(map(float, self.bins_max))

            for i in range(len(self.score_cols)):
                self.query = asign_bins(self.query,
                                        f"{self.score_cols[i]}",
                                        bins_min,
                                        bins_max)
        else:
            for i in range(len(self.score_cols)):
                score_col = self.score_cols[i]
                self.query = self.query.withColumn(f"quartile_{score_col}",
                                F.ntile(4).over(
                                    Window.orderBy(f"{score_col}")))

        self.dec_cols = [column for column, typ in self.query.dtypes \
                            if column not in self.score_cols \
                            and typ == "int"\
                            and "dec" in column]

        self.quartile_cols = [column for column, typ in self.query.dtypes \
                            if column not in self.score_cols \
                            and "quartile" in column]

        print(f"dec_cols: {self.dec_cols}, \n quartile_cols: {self.quartile_cols}")
        return self.dec_cols

    @classmethod
    def transform_scores(self: object) -> DataFrame:
        self.dec_cols = [column for column, typ in self.query.dtypes \
                    if column not in self.score_cols \
                    and "decile_" in column]

        def ascii_ignore(x):
            return x.encode('ascii', 'ignore').decode('ascii')

        ascii_udf = F.udf(ascii_ignore)

        for i in range(len(self.score_cols)):

            if len(self.score_cols) > 1:

                dec = next(itertools.dropwhile(lambda s: \
                    self.score_cols[i] not in s, self.dec_cols))

                quartile = next(itertools.dropwhile(lambda s: \
                    self.score_cols[i] not in s, self.quartile_cols))

            else:
                dec = self.dec_cols[i]
                quartile = self.quartile_cols[i]

            print(f"\n\n self.score_cols[i]: {self.score_cols[i]}")
            print(f"dec: {dec} \n\n")
            print(f"quartile: {quartile} \n\n")

            info = ["score",
                    "score_name",
                    "credit_score_quartile_val",
                    "credit_score_decile_val"]

            self.not_json = [column for column in self.query.columns \
                            if column not in info and column != self.score_cols[i]]

            print(f"Not json: {self.not_json}", "\n\n")
            cols_json = ["credit_approved_dt",
                        "credit_approved_flag",
                        "credit_score_decile_val",
                        "credit_score_quartile_val",
                        "score",
                        "score_info_dt",
                        "score_name",
                        "score_process_dt"]

            print(f"Inicia Principal Transformation .......... {i} \n ")
            self.query = self.query.withColumn("$numberDecimal", F.col(f"{dec}"))\
                        .withColumn("credit_score_decile_val",
                            F.to_json(F.struct(F.col("$numberDecimal"))))\
                        .withColumn("$numberDecimal", F.col(f"{quartile}"))\
                        .withColumn("credit_score_quartile_val",
                            F.to_json(F.struct(F.col("$numberDecimal"))))\
                        .withColumn("$numberDecimal",
                            F.col(f"{self.score_cols[i]}"))\
                        .withColumn("score",
                            F.to_json(F.struct(F.col("$numberDecimal"))))\
                        .drop("$numberDecimal")\
                        .withColumn("score_name", F.lit(f"{self.score_cols[i]}"))\
                        .select(
                            *self.not_json,
                            F.to_json(F.struct(cols_json))\
                                .alias(f"{self.score_cols[i]}"))

            print(f"{i} ................ Termina Principal Transformation \n")



        self.df_scores = self.query.select(
                    "model_type_execution",
                    "customer_id",
                    F.concat_ws(', ',
                        *self.score_cols)\
                        .alias("risk_scores"))\
                    .withColumn("risk_scores",
                        F.regexp_replace(F.col("risk_scores"), '\\\\', ''))\
                    .withColumn("risk_scores",
                        F.regexp_replace(F.col("risk_scores"), '\\"true\\"', 'true'))\
                    .withColumn("risk_scores",
                        F.regexp_replace(F.col("risk_scores"), '\\"\\{', '\\{'))\
                    .withColumn("risk_scores",
                        F.regexp_replace(F.col("risk_scores"), '\\}\"', '\\}'))\
                    .withColumn("risk_scores",
                        F.array(F.col("risk_scores")))

        return self.df_scores

    @classmethod
    def transform_segments(self: object, list_cols: List = []) -> DataFrame:

        if len(list_cols) == 0:
            list_cols.append("customer_seg")
            list_cols.append("customer_seg_dt")

            self.df_scores = self.df_scores.withColumn("customer_seg", F.lit("none"))\
                                .withColumn("customer_seg_dt",
                                    F.to_json(F.struct(
                                        F.lit(f"{self.last_updated}").alias("$date"))))

        not_cols = [columna for columna in self.df_scores.columns \
                            if columna not in list_cols]

        self.df_json = self.df_scores.select(*not_cols,
                                F.to_json(
                                    F.struct(["customer_seg", "customer_seg_dt"]))\
                                    .alias("customer_segments"))\
                                .withColumn("customer_segments",
                                    F.regexp_replace(F.col("customer_segments"), '\\\\', ''))\
                                .withColumn("customer_segments",
                                    F.regexp_replace(F.col("customer_segments"), '\\"\\{', '\\{'))\
                                .withColumn("customer_segments",
                                    F.regexp_replace(F.col("customer_segments"), '\\}\"', '\\}'))\
                                .withColumn("customer_segments",
                                    F.array(F.col("customer_segments")))

        return self.df_json

    @classmethod
    def to_hive(self: object) -> NoReturn:
        logger = logging.getLogger(__name__)
        columnas = ["model_type_execution",
                    "customer_id",
                    "risk_scores",
                    "customer_segments"]
        try:
            df = self.df_json.select(columnas)
            df.registerTempTable("df_final")
            query_str = f"""INSERT OVERWRITE TABLE {self.big_table_score}
                        PARTITION(last_updated_date = '{self.last_updated_date}', model_id = '{self.model_id}')
                        SELECT * FROM df_final """
            print(query_str)
            self.spark.sql(query_str).show()
            logger.info(f"The score table {self.model_score_table} was succesfully written into {self.big_table_score}.")
        except Exception as e:
            logging.error(f"Error al subir la tabla a Hive: \n {e}")

    @classmethod
    def transform_and_loadall(self: object) -> DataFrame:
        print("Generate attributes and the principal query")
        self.query_score()
        print("Query Transformed")
        self.transform_measure()
        print("Decile calculated")
        self.transform_scores()
        print("Transformation of scores")
        self.transform_segments()
        print("Insert to hive")
        self.to_hive()
