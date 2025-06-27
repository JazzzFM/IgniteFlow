# !/usr/bin/env python
# -*- coding: utf-8 -*- version: 3.7 -*-

from pyspark.ml.feature import QuantileDiscretizer
from typing import Dict, List, Union, NoReturn
from dataclasses import dataclass, field
#from multiprocessing.pool import Threadpool
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import DataFrame
from cytoolz.functoolz import partial
#import metrics as met_cons
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
import os


@dataclass
class MRMStatistics:
    spark: object
    dict_constants: Dict
    pivote: DataFrame = field(init = False)
    feature: DataFrame = field(init = False)
    score: DataFrame = field(init = False)
    csi_dict: DataFrame = field(init = False)
    predictors: DataFrame = field(init = False)

    @classmethod
    def create_csi_columns(df: DataFrame, prefix: str = 'decile') -> DataFrame:
        """
        Description:
                Funcion que genera las variables con los deciles asociados a los cortes de
                los datos de development para los 10 Features del modelo y el score

        Parameters:
                df: Spark dataframe con los 10 Features + Score del mes al cual se requiere
                    calcular el CSI
                csi_dict: diccionario con los cortes predefinos (y fijos) de los
                            datos de development
                prefix: prefijo con el que se nombraran las variables con los deciles

        Attributes:
                df: Retorna el dataframe origina junto con las columnas de deciles caluladas
        """
        for feat_name in self.csi_dict.keys():
            csi_boundaries = csi_dict[feat_name]
            csi_list = [csi_boundaries[x] for x in sorted(csi_boundaries.keys())]

            cond = ".".join(
                        "otherwise(F.when((F.col(feat_name) > csi_boundaries[\'" +\
                         str(x) + "\'])&(F.col(feat_name) <= csi_boundaries[\'" +\
                         str(x + 1) + "\'])," +\
                         str(len(csi_list) - x + 1) + ")"\
                            for x in range(1, len(csi_list)))

            left_cond = "F.when(F.col(feat_name).isNull(),None)" +\
                        ".otherwise(F.when(F.col(feat_name) <= " +\
                        " csi_boundaries['1'], len(csi_list) + 1)"

            right_cond = "otherwise(F.when(F.col(feat_name) > " +\
                        "csi_boundaries[str(sorted(csi_boundaries.keys())[-1])], 1)" +\
                        ".otherwise(None)))" + ")"*(len(csi_list) - 1)

            cond_full = ".".join([left_cond, cond, right_cond])
            self.df_csi = df.withColumn(f"{prefix}_{feat_name}", eval(cond_full))

        return self.df_csi


    @classmethod
    def summary2csv(self, df, show_quantiles = False, probs = [0.25, 0.5, 0.75], q_err = 0.05, exclude_col_quantiles = [], concurrent = False, number_threads = 8, feat_dict = None, data = None):
        """
        This function computes the summary of a certain Spark Dataframe and saves
        it to CSV, on the specified path_filename

        Parameters:
            df: Spark data frame, to describe
            path_filename: string, the summary will be saved there in CSV format
            show_quantiles: boolean, if True, quantiles will be added to the summary
            probs: list of floats, each one is a probability between 0 and 1,
            that will define a quantile column
            q_err: float, maximum error (in probability) when computing quantiles.
            Setting this parameter to 0 is EXTREMELY expensive.
            exclude_col_quantiles: list of string names of the columns to exclude
            from the quantile report names of the columns to exclude from
            the quantile report
            concurrent: boolean. If True, the quantile calculations will be submitted,
            in multiple threads to run them simultaneously
            number_threads: integer, number threads to consider, more allows to submit
            more tasks simultaneously, but requires more Spark resources.
            Recommended value: 7-9 threads.
            feat_dict: Opcional (defaul: None), diccionario con el Feature ID, agrega
            una columna con esta informacion
            data: Opcional (defaul: None) agrega una columna para identificar a los datos,
                 pasados a la funcion:['DEV','ITV', etc]
        """

        SUMMARY_COLUMN = "summary"
        VARIABLE_COLUMN = "variable"
        Q_COLS_PREFIX = "q_"

        summary_df = df.describe()\
                     .toPandas()\
                     .set_index(SUMMARY_COLUMN)\
                     .transpose()

        summary_df.index.name = VARIABLE_COLUMN

        if show_quantiles:
            cols = [col for col in df.columns if col not in exclude_col_quantiles]

         # Defining a function that computes quantiles for a column and prepends the column name
            def compute_quantiles(col, df, probs, error):
                # print(f"######################### {col} #########################")
                quantiles = [col] + df.approxQuantile(col = col,
                probabilities = probs,
                relativeError=error)
                return quantiles

            if concurrent:
                thread_pool = ThreadPool(number_threads)
                fun = partial(compute_quantiles, df, probs, error = q_err)
                list_quantiles_per_col = thread_pool.map(fun, cols)
                thread_pool.close()

            else:
                list_quantiles_per_col= []

            for col in cols:
                quantiles = compute_quantiles(col, df, probs, error = q_err)
                list_quantiles_per_col.append(quantiles)

            quantiles_df = pd.DataFrame(list_quantiles_per_col,
                                     columns = [VARIABLE_COLUMN] +\
                                         [Q_COLS_PREFIX + str(p) for p in probs])\
                                     .set_index(VARIABLE_COLUMN)

            results_df = summary_df.join(quantiles_df, how = 'outer')\
                                 .reset_index()

            # Reordering columns to their appearance order in DF:
            results_df[VARIABLE_COLUMN] = results_df[VARIABLE_COLUMN]\
                                         .astype("category")\
                                         .cat.set_categories(df.columns)

            results_df['nmiss'] = (df.count() - results_df['count'].astype(int))
            results_df.rename(columns = {'variable':'Feature'}, inplace = True)
            VARIABLE_COLUMN = 'Feature'

            if feat_dict == None:
                results_df = results_df.sort_values(VARIABLE_COLUMN)\
                             .set_index(VARIABLE_COLUMN)
            else:
                results_df['Feature_ID'] = results_df['Feature']\
                                         .map(feat_dict)

                cols =  ['Feature_ID',
                         'Feature',
                         'count',
                         'nmiss',
                         'stddev',
                         'mean',
                         'min'] +\
                     [Q_COLS_PREFIX + str(p) for p in probs] +\
                     ['max']

                if data != None:
                    results_df['data'] = data
                    cols_fin = ['data'] + cols
                else:
                    cols_fin = cols

            results_df = results_df[cols_fin]\
                             .sort_values('Feature_ID')\
                             .set_index('Feature_ID')
        else:
            results_df = summary_df

        # results_df.to_csv(path_filename)
        return results_df


    def csi_summary(self, train, df, key, csi_dict, prefix = 'decile'):
        """
        Description:
                Funcion que genera el summary por decil y overall con el calculo de CSI
                de score y los 10 features

        Parameters:

                train: Spark dataframe con los 10 Features + Score (datos de entrenamiento)
                df: Spark dataframe con los 10 Features + Score del mes al cual se requiere calcular el CSI
                key: variable por la cual se hara count (llave)
                csi_dict: diccionario con los cortes predefinos (y fijos) de los datos de development
                prefix: prefijo con el que se nombraran las variables con los deciles

        Attributes:
                csi_by_decil: pandas dataframe con el CSI por decil de todas las variables
                csi_overall: pandas dataframe con el CSI de todas las variables
        """

        csi_train = pd.DataFrame()
        csi_df = pd.DataFrame()

        for feat_name in csi_dict.keys():
            feat_id = feature_id_dict[feat_name]

            # Valores de los datos de entrenamiento (Expected)
            agrupado_train = train.groupBy(f"{prefix}_{feat_name}")\
                                  .agg(
                                    F.min(feat_name).alias('min_dev'),
                                    F.max(feat_name).alias('max_dev'),
                                    F.count(key).alias('count_dev'))\
                                .orderBy(f"{prefix}_{feat_name}")

            agrupado_train = agrupado_train.toPandas()
            agrupado_train.rename(columns = {f"{prefix}_{feat_name}": 'decile'},
                                                inplace = True)
            agrupado_train['Feature_ID'] = feat_id
            agrupado_train['Feature'] = feat_name
            agrupado_train['dist_dev'] = agrupado_train['count_dev']/\
                                            agrupado_train['count_dev']\
                                            .sum()

            csi_train = csi_train.append(agrupado_train)

            # Valores de los datos nuevos (Actuals)
            agrupado_df = df.groupBy(f"{prefix}_{feat_name}")\
                            .agg(F.count(key).alias('count_new'))\
                            .orderBy(f"{prefix}_{feat_name}")

            agrupado_df = agrupado_df.toPandas()
            agrupado_df.rename(columns = {f"{prefix}_{feat_name}": 'decile'},
                                            inplace = True)
            agrupado_df['Feature_ID'] = feat_id
            agrupado_df['Feature'] = feat_name
            agrupado_df['dist_new'] = agrupado_df['count_new']/\
                                        agrupado_df['count_new']\
                                        .sum()

            csi_df = csi_df.append(agrupado_df)

        csi_by_decil = pd.merge(csi_train,
                                csi_df,
                                on = ['Feature_ID','Feature','decile'],
                                how = 'inner')

        csi_by_decil['csi'] = (csi_by_decil['dist_new'] - csi_by_decil['dist_dev']) *\
                                np.log(csi_by_decil['dist_new']/csi_by_decil['dist_dev'])

        layout = ['Feature_ID',
                    'Feature',
                    'decile',
                    'min_dev',
                    'max_dev',
                    'count_dev',
                    'dist_dev',
                    'count_new',
                    'dist_new',
                    'csi']

        csi_by_decil = csi_by_decil[layout]\
                        .sort_values(by = ['Feature_ID', 'decile'])

        csi_overall = csi_by_decil\
                         .groupby(['Feature_ID','Feature'])\
                        .sum()\
                        .reset_index()

        dict_csi = {"csi_by_decil": csi_by_decil,
                    "csi_overall": csi_overall[['Feature_ID','Feature','csi']]}

        return dict_csi

    @classmethod
    def LogLoss(self, df, target: str, score: str):
        """
        Description:
                Funcion que el log_loss de un modelo de clasificacion binaria

        Parameters:
            df: spark dataframe con las columnas de score y target
            target: target column name
            score: score column name

        Attributes:
            log_loss = -(1/N)*(\\sum_{i = 1}_{N} (y_i * \\log(p(y_i)) +
                        (1-y_i) * \\log(1 - p(y_i))))
        """

        N = df.count()
        df_target = df.select([target, score])\
                    .withColumn('score_0', (F.lit(1) - F.col(score)))\
                    .withColumn('log_p', F.log(F.col(score)))\
                    .withColumn('log_q', F.log(F.col('score_0')))

        log_p = df_target.where(F.col(target) == 1)\
                    .agg({'log_p': 'sum'})\
                    .collect()[0][0]

        log_q = df_target.where(F.col(target) == 0)\
                    .agg({'log_q': 'sum'})\
                    .collect()[0][0]

        self.log_loss = -(1/float(N))*(log_p + log_q)

        return self.log_loss

    @classmethod
    def performance(self, df, target, score, key, decile_col, data = None, csi = None):
        """
        Description:

        Funcion que genera el summary con el performance por decil del score

        Parameters:
            df: Spark dataframe que contiene la target, score, key y
                la variable de deciles con las cuales se calcuara el performance
            target: Target column name
            score: Score column name
            key: ID at table level
            decile_col: decile column name, con esta se calcula el performance
            data: opcional, string identificador de los datos pasados a la funcion

        Attributes:
            dict(agg_perf, perf_overall): Performance del modelo x decil
                                            (KS, MAPE, ROB, Lift, etc) y overall
        """

        agg_perf = df.groupBy(decile_col)\
                    .agg(
                        F.count(key).alias('nbin'),
                        F.min(score).alias('min_score'),
                        F.max(score).alias('max_score'),
                        F.mean(score).alias('mean_score'),
                        F.sum(target).alias('target'))\
                    .orderBy(decile_col)\
                    .toPandas()

        agg_perf.rename(columns = {decile_col: 'decile'}, inplace = True)
        agg_perf['distBin'] = agg_perf['nbin']/agg_perf['nbin'].sum()
        agg_perf['meanTarget'] = agg_perf['target']/agg_perf['nbin']
        agg_perf['noTarget'] = (agg_perf['nbin'].astype(int) - agg_perf['target'].astype(int))
        agg_perf['distTarget'] = agg_perf['target']/agg_perf['target'].sum()
        agg_perf['distCumBin'] = agg_perf['distBin'].cumsum()
        agg_perf['distCumTarget'] = agg_perf['distTarget'].cumsum()
        agg_perf['distCumNoTarget'] = (agg_perf['noTarget']/agg_perf['noTarget'].sum()).cumsum()
        agg_perf['ROB'] = (agg_perf['meanTarget'] != np.sort(agg_perf['meanTarget'])[::-1])
        agg_perf['KS'] = abs(agg_perf['distCumTarget']-agg_perf['distCumNoTarget'])
        agg_perf['MAPE'] = abs((agg_perf['mean_score']/agg_perf['meanTarget'])-1)
        agg_perf['Lift'] = agg_perf['distTarget']/agg_perf['distBin']

        log_loss = self.LogLoss(df, target, score)

        cols = ['decile',
                'nbin',
                'distBin',
                'min_score',
                'max_score',
                'mean_score',
                'meanTarget',
                'target',
                'noTarget',
                'distTarget',
                'distCumBin',
                'distCumTarget',
                'distCumNoTarget',
                'ROB',
                'KS',
                'MAPE',
                'Lift']

        cols_summary = ['KS',
                        'MAPE',
                        'ROB',
                        'meanTarget',
                        'mean_score',
                        'pct_error']

        summary = [data,
            agg_perf.KS.max(),
            agg_perf.MAPE.mean(),
            agg_perf.ROB.max(),
            agg_perf.meanTarget.mean(),
            agg_perf.mean_score.mean(),
            (agg_perf.meanTarget.mean() - agg_perf.mean_score.mean())/\
                agg_perf.meanTarget.mean()]

        if data != None:
            agg_perf['data'] = data
            cols = ['data'] + cols
            agg_perf = agg_perf[cols]

            if csi is not None:
                cols_summary = ['data'] + cols_summary + ['PSI','Log-loss']
                summary += [csi['csi'][csi['Feature'] == score][0], log_loss]

            else:
                cols_summary = ['data'] + cols_summary + ['Log-loss']
                summary += [log_loss]

        else:
            agg_perf = agg_perf[cols]

            if csi is not None:
                cols_summary += ['PSI','Log-loss']
                summary += [csi['csi'][csi['Feature'] == score][0],
                            log_loss]
            else:
                cols_summary += ['Log-loss']
                summary += [log_loss]


        perf_overall = pd.DataFrame(columns = cols_summary)
        perf_overall.loc[len(cols_summary)] = summary

        if data == None:
            perf_overall = perf_overall.T
        else:
            perf_overall = perf_overall\
                            .set_index('data').T

        dict_perf = {"agg_perf": agg_perf,
                    "perf_overall": perf_overall}

    @classmethod
    def make_dict_decile(self, df, inputCol, outputCol = 'buckets', buckets = 10, error = 0.0001):
        """
        Description:
            Funcion que genera un diccionario con los valores de cortes por decil
            de la varaiable pasada a la funcion, este diccionario se utiliza para
            construir la columna decile_<feature> con los cortes definidos aqui.

        Parameters:
            df: Spark dataframe con el inputCol que sera discretizado
            inputCol: column name
            outputCol: opcional, default: 'buckets'
            buckets: opcional, default: 10
            error: opcional, default: 0.0001

        Attributes:
            dict_decile: Diccionario con los cortes por decil

        """

        df_decile_column = df.select(inputCol)
        qds = QuantileDiscretizer(numBuckets = buckets,
                                    inputCol = inputCol,
                                    outputCol = outputCol,
                                    relativeError = error,
                                    handleInvalid="error")

        bucketizer = qds.fit(df_decile_column)
        df_buckets = bucketizer.setHandleInvalid("skip")\
                            .transform(df_decile_column)

        agg_max = df_buckets.groupBy(outputCol)\
                            .agg(F.max(inputCol).alias('max_'))\
                            .orderBy(outputCol)\
                            .toPandas()

        n = len(agg_max)-1
        agg_max = agg_max.iloc[0:n,:]
        agg_max[outputCol] = agg_max[outputCol].astype('int')+1

        self.dict_decile = {inputCol: dict(zip(
                                        agg_max[outputCol].astype('str'),
                                        agg_max.max_ ))}

        return self.dict_decile


@dataclass
class CreateMetrics(MRMStatistics):

    def __post_init__(self: object) -> NoReturn:
        dict_pivote = self.dict_constants.get("pivot")
        print(dict_pivote, "\n")
        self.pivote = self.load_input_metrics(input_dict_name = dict_pivote)
        self.pivote.printSchema()
        self.pivote_key =  self.dict_constants.get("pivot").get("key")
        print("\n")

        dict_feature = self.dict_constants.get("feature")
        print(dict_feature, "\n")
        self.feature = self.load_input_metrics(input_dict_name = dict_feature)
        self.feature.printSchema()
        self.feature_key = self.dict_constants.get("feature").get("key")
        self.feature_as_type = self.dict_constants.get("feature").get("type")
        self.feature_vars_select = self.dict_constants.get("feature").get("vars_select")
        self.feature_column = self.dict_constants.get("feature").get("feature_column")
        print("\n")

        dict_score = self.dict_constants.get("score")
        print(dict_score, "\n")
        self.score = self.load_input_metrics(input_dict_name = dict_score)
        self.score.printSchema()
        self.score_key =  self.dict_constants.get("score").get("key")
        print("\n")

        print("CSI DICT: ")
        self.csi_dict = self.dict_constants.get("csi_dict")
        print(self.csi_dict, "\n")

        print("PREDICTORS:")
        self.predictors = self.dict_constants.get("predictors")
        print(self.predictors, "\n")

    def load_input_metrics(self: object, input_dict_name: Dict) -> DataFrame:
        name = input_dict_name.get("name")
        type_table = input_dict_name.get("type_table")
        vars_select = input_dict_name.get("vars_select")
        partition = input_dict_name.get("partition")
        proc_dt = input_dict_name.get("proc_dt")
        key = input_dict_name.get("key")

        print(f"{type(vars_select)}")
        if str(type(vars_select)) == "<class 'list'>":
            vars_select = ", ".join(vars_select)

        if type_table == "parquet":
            df = self.spark.read.parquet(f"{name}")

        elif type_table == "hive":
            if proc_dt == "last":
                df = self.spark.sql(f"""SELECT {key}, {vars_select}
                    FROM {name}
                    WHERE {partition} = (SELECT MAX({partition})
                        FROM {name})""")
            else:
                df = self.spark.sql(f"""SELECT {key}, {vars_select}
                    FROM {name}
                    WHERE {partition} = '{proc_dt}' """)
        else:
            print("Elegir entre hive o parquet")
        return df

    def perepare_data_predicted(self: object) -> DataFrame:
        # Step 1
        # Get info from final score table
        self.score = self.score.withColumnRenamed(f"{self.score_key}", "customer_id")\
                            .withColumn("customer_id", F.col("customer_id").cast("bigint"))

        # Calculate column predicted based on the model
        self.feature = self.feature.withColumn("numcliente",
                            F.col(f"{self.feature_key}")\
                            .cast("bigint"))

        if self.feature_as_type == "string":
            self.feature = self.feature.select("numcliente",
                            F.split(F.col("all_features_vector"), ",").alias('column_to_be_split'))
            df_sizes = self.feature.select(F.size('column_to_be_split').alias('column_to_be_split'))
            n_columns = df_sizes.agg(F.max('column_to_be_split')).collect()[0][0]
            self.feature = self.feature.select("numcliente",
                                *[self.feature['column_to_be_split'][i] for i in range(n_columns)])

            total_columns = [x for x in self.feature.columns if x != "numcliente"]

            for i in range(len(total_columns)):
                self.feature = self.feature.withColumnRenamed(total_columns[i], self.predictors[i])\
                                .withColumn(self.predictors[i],
                                    F.col(self.predictors[i]).cast("Double"))

        elif self.feature_as_type == "vector":
            print("vector")

    def get_data_predicted(self: object, data_predicted: DataFrame, id_col: str) -> DataFrame:
        # Step2: El calculo se debe hacer desde afuera
        # Threshold -> 0, 1 conseguir data predicted
        self.data_predicted = data_predicted\
                                .withColumn("customer_id",
                                    F.col(f"{id_col}").cast("bigint"))
        return self.data_predicted


    @classmethod
    def create_input_metrics_monitor(self: object) -> DataFrame:
        # Deleate historic step, it is only for performance
        input_metrics = self.score.join(self.data_intermediate_output,
                            [self.score.customer_id == self.data_intermediate_output.numcliente],
                            how = "inner")\
                            .drop("numcliente", "cohort_date")

        self.input_metrics = input_metrics.join(self.data_predicted,
                            on = "customer_id",
                            how = "inner")

        return self.input_metrics

    @classmethod
    def create_input_metrics_historic(self: object, pivot_date: str) -> DataFrame:
        pass

    @classmethod
    def create_csi_columns(self: object) -> DataFrame:
        # Create/modify other columns
        input_metrics = input_metrics.withColumnRenamed("seg","segment")\
                                    .withColumnRenamed("pyrl_atr_inf_dt","inf_dt")\
                                    .withColumn("tef",
                                        F.col("tef").cast("bigint"))\
                                    .withColumn("data",
                                        F.col("inf_dt"))\
                                    .withColumn("importe",
                                        F.col("importe").cast("bigint"))

        input_metrics = self.create_csi_columns(input_metrics, self.csi_dict)
        input_metrics = input_metrics.withColumn("process_date",
                            F.current_date()\
                            .cast("string"))

        input_metrics.schema['process_date'].nullable = True
        self.input_metrics = input_metrics.select(*final_vars)

        return input_metrics


    @classmethod
    def create_statistics_metrics_table(self, df, vars_features, probs_list, feature_id_dict, tbl_date, final_vars):
        # Get statistics of each feature
        input_metrics_ft = df.select(*vars_features)
        table_pandas = self.summary2csv(df = input_metrics_ft,
                            show_quantiles = True,
                            probs = probs_list,
                            feat_dict = feature_id_dict)

        table_pandas = table_pandas.reset_index()
        new_table = self.spark.createDataFrame(table_pandas)

        new_table = new_table.withColumnRenamed("Feature_ID","feature_id")\
                        .withColumnRenamed("Feature","variable")\
                        .withColumnRenamed("q_0.01","q_1")\
                        .withColumnRenamed("q_0.05","q_5")\
                        .withColumnRenamed("q_0.1","q_10")\
                        .withColumnRenamed("q_0.25","q_25")\
                        .withColumnRenamed("q_0.5","q_50")\
                        .withColumnRenamed("q_0.75","q_75")\
                        .withColumnRenamed("q_0.9","q_90")\
                        .withColumnRenamed("q_0.95","q_95")\
                        .withColumnRenamed("q_0.99","q_99")\
                        .withColumnRenamed("nmiss","n_miss")\
                        .withColumn("feature_id",
                            F.col("feature_id").cast("integer"))\
                        .withColumn("count",
                            F.col("count").cast("bigint"))\
                        .withColumn("mean",
                            F.col("mean").cast("double"))\
                        .withColumn("stddev",
                            F.col("stddev").cast("double"))\
                        .withColumn("min",
                            F.col("min").cast("double"))\
                        .withColumn("max",
                            F.col("max").cast("double"))\
                        .withColumn("inf_dt",
                            F.lit(tbl_date))\
                        .withColumn("data",
                            F.lit(tbl_date))\
                        .withColumn("process_date",
                            F.current_date().cast("string"))

        #Get statistics of each feature
        new_table = new_table.select(*final_vars)
        return new_table

    @classmethod
    def create_csi_tables(self, csi_by_decil, csi_overall, path_relative_importance,
        tbl_date, final_vars_csi_by_decil, final_vars_csi_overall):


        table_csi_by_decil = self.spark.createDataFrame(csi_by_decil)\
                                .withColumnRenamed("Feature_ID","feature_id")\
                                .withColumnRenamed("Feature","variable")\
                                .withColumnRenamed("decile","bin")\
                                .withColumnRenamed("count_new","nbin")\
                                .withColumnRenamed("dist_new","pct_nbin")\
                                .withColumn("feature_id",
                                    F.col("feature_id").cast("integer"))\
                                .withColumn("data",
                                    F.lit(tbl_date))\
                                .withColumn("inf_dt",
                                    F.lit(tbl_date))\
                                .withColumn("process_date",
                                    F.current_date().cast("string"))\
                                .select(*final_vars_csi_by_decil)

        table_csi_overall = self.spark.createDataFrame(csi_overall)\
                                .withColumnRenamed("Feature_ID","feature_id")\
                                .withColumnRenamed("Feature","variable")\
                                .withColumn("feature_id",
                                    F.col("feature_id").cast("integer"))

        # Read parquet relative importance
        data_rel_imp = self.spark.read.parquet(path_relative_importance)\
                            .drop("index")

        schema_scor_rel_imp = StructType([StructField("variable", StringType(),True),
                    StructField("relative_importance", DoubleType(), True)])

        data_scor_rel_imp = [("pyrl_atr_scor", None)]
        df_scor_rel_imp = self.spark.createDataFrame(data_scor_rel_imp,
                                schema_scor_rel_imp)

        data_rel_imp = df_scor_rel_imp.union(data_rel_imp)

        table_psi = data_rel_imp.join(table_csi_overall,
                            on = "variable",
                            how = "inner")\
                        .withColumn("inf_dt",
                            F.lit(tbl_date))\
                        .withColumn("data",
                            F.lit(tbl_date))\
                        .withColumn("process_date",
                            F.current_date().cast("string"))\
                        .select(*final_vars_csi_overall)

        return table_csi_by_decil, table_psi

    @classmethod
    def create_performance_tables(self, df, df_csi_overall, tbl_date, final_vars_decil,
        final_vars_overall, saveToBase_2, saveToTable_3):

        perf_new, summary_new = self.model_performance(df = df,
                                    target = 'target',
                                    score = 'pyrl_atr_scor',
                                    key = 'customer_id',
                                    decile_col = 'decile_new_pyrl_atr_scor',
                                    csi = df_csi_overall)

        table_5 = self.spark.createDataFrame(perf_new)\
                    .withColumn("decile",F.col("decile").cast("integer"))\
                    .withColumnRenamed("nbin","n_bin")\
                    .withColumnRenamed("distBin","dist_bin")\
                    .withColumnRenamed("min_score","min_prob")\
                    .withColumnRenamed("max_score","max_prob")\
                    .withColumnRenamed("mean_score","avg_prob")\
                    .withColumnRenamed("meanTarget","avg_target")\
                    .withColumnRenamed("noTarget","no_target")\
                    .withColumnRenamed("distTarget","dist_target")\
                    .withColumnRenamed("distCumBin","dist_cum_bin")\
                    .withColumnRenamed("distCumTarget","dist_cum_target")\
                    .withColumnRenamed("distCumNoTarget","dist_cum_no_target")\
                    .withColumnRenamed("ROB","rob")\
                    .withColumnRenamed("KS","ks")\
                    .withColumnRenamed("MAPE","mape")\
                    .withColumnRenamed("Lift","lift")\
                    .withColumn("bucket",
                        F.concat(F.lit("("),
                            F.col("min_prob").cast("string"),
                            F.lit(", "),
                            F.col("max_prob").cast("string"),
                            F.lit("]")))\
                    .withColumn("inf_dt",F.lit(tbl_date))\
                    .withColumn("data",F.lit(tbl_date))\
                    .withColumn("process_date",
                        F.current_date().cast("string"))\
                    .select(*final_vars_decil)

        # Get ks corresponding to DEV
        performance_ks_dev = sqlContext.sql(f"SELECT ks FROM {table_1} WHERE data = 'DEV' ")
        ks_dev = performance_ks_dev.select("ks").collect()[0][0]

        # Get ks corresponding to ITV
        performance_ks_itv = sqlContext.sql(f"SELECT ks FROM {table_1} WHERE data = 'ITV' ")
        ks_itv = performance_ks_itv.select("ks").collect()[0][0]

        summary_new_transpose = summary_new.T

        table_1 = self.spark.createDataFrame(summary_new_transpose)\
                    .withColumnRenamed("KS","ks")\
                    .withColumnRenamed("MAPE","mape")\
                    .withColumnRenamed("ROB","rank_order_brake")\
                    .withColumnRenamed("meanTarget","avg_target")\
                    .withColumnRenamed("mean_score","avg_probability")\
                    .withColumnRenamed("PSI","psi")\
                    .withColumnRenamed("Log-loss","log_loss")\
                    .withColumn("data",
                        F.lit(tbl_date))\
                    .withColumn("ks_variation_vs_dev",
                        ((F.col("ks")/ks_dev) - 1))\
                    .withColumn("ks_variation_vs_itv",
                        ((F.col("ks")/ks_itv) - 1))\
                    .withColumn("rank_order_brake",
                        F.col("rank_order_brake").cast("string"))\
                    .withColumn("inf_dt",
                        F.lit(tbl_date))\
                    .withColumn("process_date",F.current_date().cast("string"))\
                    .select(*final_vars_overall)

        return table_5, table_1

@dataclass
class MetricsMonitor(CreateMetrics):

    @classmethod
    def to_hive(spark: object, config: object, df, table_name: str) -> NoReturn:
        logger = logging.getLogger(__name__)
        today = datetime.today().strftime("%Y-%m-%d")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict").show()
        try:
            features.registerTempTable("final_table")
            spark.sql(f"""INSERT OVERWRITE TABLE {table_name}
                    PARTITION (proc_dt = '{today}')
                    SELECT * FROM final_table""").show()
        except Exception as e:
            logger.error(f"Error al subir la tabla a Hive: \n {e}")

    def create_input_metrics_monitor(self) -> NoReturn:
        """
        Debemos entender esta función, de ser necesario redefinirla

            - Esta función sólo regresa el query con la petición del score a cierta fecha data
                Hay que entender que fecha toma con respecto al today()
        """
        self.perepare_data_predicted()

        print("PIVOTE: \n")
        self.pivote.show(10)

        print("FEATURE: \n")
        self.feature.show(10)

        print("SCORE: \n")
        self.score.show(10)



    def pipeline_table_two(self: object) -> NoReturn:
        # Filter by pyrl_atr_f equals 1
        # input_metrics_hist_flag = input_metrics_hist.where(F.col("pyrl_atr_f") == 1)
        # input_metrics_flag = input_metrics.where(F.col("pyrl_atr_f") == 1)

        # Calculate cap & floor
        dist_capfloor_hist = self.summary2csv(
            df = input_metrics_hist_flag.select(features_list),
            show_quantiles = True,
            probs = [0.01, 0.99],
            q_err = 0.0001,
            exclude_col_quantiles = [],
            concurrent = True,
            number_threads = 8,
            feat_dict = feature_id_dict)

        df_camelot_cf_hist = metricas.cap_floor(
            df = input_metrics_hist_flag,
            distributions = dist_capfloor_hist)

        dist_capfloor = self.summary2csv(
            df = input_metrics_flag.select(features_list),
            show_quantiles = True,
            probs = [0.01, 0.99],
            q_err = 0.0001,
            exclude_col_quantiles = [],
            concurrent = True,
            number_threads = 8,
            feat_dict = feature_id_dict)

        df_camelot_cf = self.cap_floor(
            df = input_metrics_flag,
            distributions = dist_capfloor)

        # T2 - Statistics Features Historic

        vars_features = met_cons.vars_features
        probs_features = met_cons.probs_features
        sorted_vars_features = met_cons.sorted_vars_features

        self.table_2 = self.create_statistics_metrics_table(
            df = df_camelot_cf_hist,
            vars_features = vars_features,
            probs_list = probs_features,
            feature_id_dict = feature_id_dict,
            tbl_date = date_hist,
            final_vars = sorted_vars_features,
            sqlContext = sqlContext)


    def pipeline_table_three_four(self: object) -> NoReturn:
        pass

    def pipeline_table_one_five(self: object) -> NoReturn:
        pass

    def run_all(self: object) -> NoReturn:
        self.create_input_metrics_monitor()



@dataclass
class MetricsPerformance(MetricsMonitor):

    def __post_init__(self: object, InputDictName: Dict) -> NoReturn:
        pass
