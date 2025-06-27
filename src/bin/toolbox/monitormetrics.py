# !/usr/bin/env python

#vintage_date = max 1er día de ese mes
#CSI FEATURES
#PSI SCORE

# -*- coding: utf-8 -*- version: 3.7 -*-
# DQC


from ast import Raise
from email import header
from unittest.result import failfast
import resource
import logging
from pyspark.sql.functions import col, when, split, regexp_replace, count, coalesce, sum, log, lit, to_date, max, mean, stddev, min, expr, round, sys
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, SparkSession, functions as F
from datetime import datetime
from functools import reduce
from operator import add
from typing import Dict
from toolbox_temp.mail import EmailSender

class MonitorMetrics():

    def __init__(self, spark, dict_metrics, date = None):
        self.date = date
        self.spark = spark
        self.dict_metrics = dict_metrics
        #self.psi_csi_calculation("csi")
        #x,y,z = self.data_format()
        #self.valida_total_csi_psi(y)


    # ***************************************************************************
    # Function spcifically to obtain the Feature of the use case INTL
    ## Data Cleansing and Conversion specifically for INTL model
    # ***************************************************************************
    def intl_features(self, df: DataFrame, variables: dict):
        # The features are saved in a whole string in the field named "all_features_vector so the following data conversions transform and clean the data in order to be used in the process"
        # Removes the "[]" characters from the all_features_vector field
        df = df.withColumn("all_features_vector", regexp_replace(col("all_features_vector"), r"[\[\]]", ""))
        # Split the 51 features that are stored in all_features_vector field separated by a "," and then is converted in a string array
        df = df.withColumn("all_features_vector", split(col("all_features_vector"), ","))
        # In order to work with the data is needed to be casted from string array to double array
        df = df.withColumn("all_features_vector", col("all_features_vector").cast("array<double>"))
        # This function counts the total features stored in the field all_features_vector giving us a total of 51
        max_array_size = df.selectExpr("max(size(all_features_vector))").collect()[0][0]

        # Having the max array size now we can work with the data.
        for i in range(max_array_size):
            # This process starts to put every item from the all_feaatures_vector array classifying to its respective feature column
            df = df.withColumn(list(variables.keys())[i], col("all_features_vector").getItem(i))

        return df

    def rps_variables_rename(self, df: DataFrame):
        df_renamed = df.withColumnRenamed("weightedrevolvingbalanceratio", "wr_rev" )\
                .withColumnRenamed("sixmonthincomemeannumber","mean_nim_6m")\
                .withColumnRenamed("threemonthcreditcardlogmeannumber","logmean_no_crd_bnmx_3m")\
                .withColumnRenamed("sixmonthrevolvingaccountmeannumber","mean_no_rev_crd_bnmx_6m")\
                .withColumnRenamed("sixmonthrevenuelogmeannumber","logmean_tnr_6m")\
                .withColumnRenamed("sixmonthrewardstandarddeviationnumber","stdd_ar_6m")\
                .withColumnRenamed("sixmonthrevenuestandarddeviationnumber","stdd_tnr_6m")\
                .withColumnRenamed("sixmonthtransactionsalesmeannumber","mean_salestx_6m")\
                .withColumnRenamed("sixmonthpurchasesstandarddeviationnumber","stdd_salestx_6m")\
                .withColumnRenamed("threemonthmaxcreditlinenumber","max_tot_cl_dept_3m")

        return df_renamed
    # This function is ued for load the input DataFrames in hive or parquet formats
    def load_input_metrics(self, dict_table: Dict) -> DataFrame:
            if self.date == None:
                name = dict_table.get("name")
            else:
                name = str(dict_table.get("name"))
                name = name.format(str(self.date))
            
            type_table = dict_table.get("type_table")
            vars_select = dict_table.get("vars_select")
            partition = dict_table.get("partition")
            pivot_date = self.dict_metrics.get("pivot_date")
            lag = int(self.dict_metrics.get("lag"))
            key = dict_table.get("key")

            print(name)

            if type_table == "parquet":
                
                df = self.spark.read.parquet(f"{name}")

            elif type_table == "hive":
                if pivot_date == "last":
                    df = self.spark.sql(f"""SELECT {key}, {vars_select}
                        FROM {name}
                        WHERE {partition} = (SELECT MAX({partition})
                            FROM {name})""")
                else:
                    pivot_date = datetime.strptime(pivot_date, "%Y-%m-%d").date()
                    pivot_date = pivot_date - relativedelta(months=lag)
                    df = self.spark.sql(f"""SELECT {key}, {vars_select}
                        FROM {name}
                        WHERE {partition} = '{pivot_date}' """)
            else:
                print("Elegir entre hive o parquet")
            return df

    # This function is used for generate the "count/dist dev" DataFrames the information is extracted from the metrics.json
    def generate_dev_df(self, var_name: str, count_or_dist: str, variables: Dict) -> DataFrame:
            logger = logging.getLogger(__name__)
            list_item = []
            items = variables[var_name][count_or_dist]
            logger.info(f"Calculating bin PSI/CSI")
            for k, v in items.items():
                    list_item.append((k, v))
                    bin_col_name = "{}_bin".format(var_name)
                    item_col_name = "{}_dev".format(count_or_dist)
            dev_df = self.spark.createDataFrame(list_item, [bin_col_name, item_col_name])
            if count_or_dist == "dist":
                dev_df = dev_df.withColumn(item_col_name, col(item_col_name).cast("double"))
            return dev_df

    # ***************************************************************************
    # This functions groups and obtains the total of psi/csi by variable and date
    # ***************************************************************************
    def total_csi_psi_monthly(self, df: DataFrame, calculation: str) -> DataFrame:
         logger = logging.getLogger(__name__)
         logger.info(f"Calculating Total PSI/CSI")
         grouped_df = df.groupBy("vintage_date", "variable").agg(sum(calculation).alias("total_{}".format(calculation)))
         return grouped_df

    # Data Quality Check
    def dqc(self, df: DataFrame, variable: str, vintage_date, key_count: int):
         logger = logging.getLogger(__name__)
         percentiles_list = [0.01, 0.05, 0.1, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 0.95, 0.99]
         percentiles_str_list = ["p1", "p5", "p10", "p20", "p25", "p30", "p40", "p50", "p60", "p70", "p75", "p80", "p90", "p95", "p99"]
         dqc_df = df.select(variable).agg(count("*").alias("count"), stddev(variable).alias("std"), mean(variable).alias("mean"), min(variable).alias("min"), max(variable).alias("max"))
         dqc_df = dqc_df.select(lit(variable).alias("variable"), "*")
         dqc_df = dqc_df.select(lit(vintage_date).alias("vintage_date"), "*")
         dqc_df = dqc_df.withColumn("N_Miss", key_count - dqc_df["count"])
         percentiles = df.approxQuantile(variable, percentiles_list, 0.1)
         logger.info(f"Calculating DQC")
         for i in range(len(percentiles_list)):
              dqc_df = dqc_df.withColumn(percentiles_str_list[i], lit(percentiles[i]))
         column_order = ["vintage_date", "variable", "count", "N_miss", "std", "mean", "min", *percentiles_str_list, "max"]
         ordered_df = dqc_df.select(*[col(column_name) for column_name in column_order])
         return ordered_df

    def load_config_app(self,calculation:str):
        self.table_dist = self.dict_metrics.get("out").get("tabla1")
        self.table_total = self.dict_metrics.get("out").get("tabla2")
        self.table_dqc = self.dict_metrics.get("out").get("tabla3")
        self.model_id = self.dict_metrics.get("model_id")
        self.sender = self.dict_metrics.get("mails").get("sender")
        self.recivers = self.dict_metrics.get("mails").get("recivers")
        self.model_name = self.dict_metrics.get("model_name")

        if calculation == "csi":
            selection = "feature"
        else:
            selection = "score"
        self.table_input = self.dict_metrics.get("in").get(selection)
        self.partition_fs = self.dict_metrics.get("in").get(selection).get("partition")
        self.key_fs = self.dict_metrics.get("in").get(selection).get("key")
        self.vars_fs = self.dict_metrics.get("in").get(selection).get("vars_select")
        self.vintage_fs = self.dict_metrics.get("in").get(selection).get("vintage_date")

    # The main part of the process calculate the psi/csi of each variable
    def psi_csi_calculation(self, calculation: str):
        self.load_config_app(calculation)

        # Get the variable names from psi or csi dictionary from metrics.json config file
        variables = self.dict_metrics.get("{}_dict".format(calculation))

        # Loads the raw data from the features/scores source
        csi_psi_df = self.load_input_metrics(self.table_input)
        csi_psi_df = csi_psi_df.withColumnRenamed(self.key_fs,"numcliente")\
                            .withColumnRenamed(self.vintage_fs, "vintage_date")\
                            .withColumnRenamed(self.partition_fs,"process_date")
        csi_psi_df = csi_psi_df.drop("process_date")
        #t_calculation_vintage = "vintage_date"

        # Converts the data to be used as input this conversions are specifically for the INTL model
        # This two lines should be removed for the other models
        if str(self.model_id) == "222566" and calculation == "csi":
            csi_psi_df = self.intl_features(csi_psi_df, variables)
        elif str(self.model_id) == "227756" and calculation == "csi":
            csi_psi_df = self.rps_variables_rename(csi_psi_df)

        # Obtaining vintage date
        vintage_date = csi_psi_df.agg(max("vintage_date")).collect()[0][0]

        # This list save the base bines df is part of data cleansing and functions to add the dev count and dist
        base_bines_df_list = []

        #count [TABLE KEY] in this case numcliente/datos_demograficos rows
        key_count = csi_psi_df.select(count("numcliente")).first()[0]

        # DQC DF LIST
        df_dqc_list = []

        # Main process
        ## Generating the bin classification
        # Now is time to make the comparison of each feature by its respective bin range
        for var_name, bines in variables.items():

            # Generating DQC_DF and appending to a list to be merged to a final DQC_DF
            dqc_df = self.dqc(csi_psi_df, var_name, vintage_date, key_count)
            df_dqc_list.append(dqc_df)

            # This gets the bin dictionary of its respective variable this is also stored in the metrics.json config file
            bines = variables[var_name]["bin"]
            # The conditions are going to be generated by the bines of its respective variable
            conditions = []
            # Generating DataFrame base of bines to fill NULL values at final DataFrame to its respective bin value
            base_bines_df_data = [(bin+1, 0) for bin in range(len(list(bines.keys())))]
            #print (f"variable base_bines de cada ciclo {base_bines_df_data}\n")

            base_bines_df = self.spark.createDataFrame(base_bines_df_data, ["bin", "count"])

            base_bines_df_list.append(base_bines_df)
            # Here is where the conditions are generated taking all but the last bin because the last bin is always "otherwise"
            if len(list(bines.keys())) == 1:
                csi_psi_df = csi_psi_df.withColumn("{}_bin".format(var_name), lit(list(bines.keys())[0]))
                continue
            for kbin, vbin in list(bines.items())[:-1]:
                # The conditions are stored in col data type
                                    # and are always less equals (<=)
                condition = col(var_name) <= vbin, kbin
                # And then stored in a list
                conditions.append(condition)
            # After that the conditions are stored for the variable using a lambda function and a reduce function from the functools module the conditions are grouped to be used in the next step
            combined_condition = reduce(lambda acc, cond: acc.when(cond[0], cond[1]), conditions, when(conditions[0][0], conditions[0][1]))

            # Here the conditions are applyed if none of the conditions match the bin value is the key of the last bin
            csi_psi_df = csi_psi_df.withColumn("{}_bin".format(var_name), combined_condition.otherwise(list(bines.keys())[-1]))

        # This list is going to be used to save each cleansed DataFrame
        #df_list = []
        # col names for the DataFrame
        count_col_name = "count_current"
        dist_col_name = "dist_current"
        dist_col_name_dev = "dist_dev"
        calculataion_col_name = calculation

        ## The following steps is the creation and join of the count_dev, count_current, dist_dev, dist_current DataFrames

        # This list is going to be used to save each intermediary DataFrame
        df_list = []

        # This loop counts all the bines taken from the process above [bin classification]
        i = 0
        for var_name in variables.keys():
            # bin col name for make the group by sum count of bines operation
            # Example of bin_col_name: sdo_venc_cliente_bin / var_name_bin adds the var_name + suffix _bin
            bin_col_name = "{}_bin".format(var_name)
            # In this DataFrame we count the bines of one column var_name_bin for example: sdo_venc_cliente_bin
            bin_count_df = csi_psi_df.groupBy(bin_col_name).agg(count("*").alias(count_col_name))
            # Then in order to fill NULL values with its respective default value a join operation is used using the base DataFrame from the line 19
            #merged_df = base_bines_df.join(bin_count_df, base_bines_df["bin"] == bin_count_df[bin_col_name], how="left")
            merged_df = base_bines_df_list[i].join(bin_count_df, base_bines_df_list[i]["bin"] == bin_count_df[bin_col_name], how="left")
            i += 1
          # Now to fill the NULL values the coalesce operation is used to managed the NULL data
            filled_df = merged_df.withColumn(bin_col_name, coalesce(merged_df[bin_col_name], merged_df["bin"])).withColumn(count_col_name, coalesce(merged_df[count_col_name], merged_df["count"]))
            # As the DataFrame is joined with the "base bines data frame and for the final product is not necessary is dropped now"
            filled_df = filled_df.select(bin_col_name, count_col_name)
            ## To generate the count and dist dataframes a function from the line 27
            # The count values from dev are taken from the confing.json of it's respective variable and created
            count_df = self.generate_dev_df(var_name, "count", variables)
            # The dist values from dev are taken from the confing.json of it's respective variable and created
            dist_df = self.generate_dev_df(var_name, "dist", variables)
            ## The the above generated DataFrames count and dist are joined to the filled_df
            filled_df = filled_df.join(count_df, on=bin_col_name, how="left")
            filled_df = filled_df.join(dist_df, on=bin_col_name, how="left")
            # Rename column var_bin as "bin" without the var name as a prefix
            filled_df = filled_df.withColumnRenamed(bin_col_name, "bin")
            # Adding column variable
            var_name_col = lit(var_name)
            filled_df = filled_df.select(var_name_col.alias("variable"), "*")
            # Finally the data is ordered by the bin column name
            filled_df = filled_df.orderBy(bin_col_name)
            # Adding column model_id

            # Adding vintage_date column
            vintage_date_col = lit(vintage_date)
            filled_df = filled_df.select(vintage_date_col.alias("vintage_date"), "*")
            filled_df = filled_df.withColumn("vintage_date", to_date(col("vintage_date"), "yyyy-MM-dd"))
            # Retrieves the bines to a list
            # Converts the list to an RDD and sums the total
            bines_total_items = filled_df.rdd.flatMap(lambda x: [x[count_col_name]])
            # Creates the column with the real count of bines
            bines_total_count = reduce(add, bines_total_items.collect())
            ## Creates the column with the current/real dist: count_current / bines_total_count
            filled_df = filled_df.withColumn(dist_col_name, (col(count_col_name)) / bines_total_count)
            # Creates the column csi/psi: (dist_current - dist_dev) * LN(dist_current / dist_dev)
            filled_df = filled_df.withColumn(calculataion_col_name, ((col(dist_col_name)) - col(dist_col_name_dev)) * (log((col(dist_col_name)) / col(dist_col_name_dev))))
            # Change integer type to long type in model_id column
            #filled_df = filled_df.withColumn("model_id", col("model_id").cast("integer"))
            # Change integer type to string type in bin column
            filled_df = filled_df.withColumn("bin", col("bin").cast("string"))
            # If the above calculation of the csi/psi col generates a NA because you can't divide zero by zero then the row is filled with the number 0.0
            filled_df = filled_df.fillna(0.0)

            # Append each df to a list
            df_list.append(filled_df)

        # Merge the DataFrames using a lambda function and the respective list of DataFrames
        final_df = reduce(lambda df1, df2: df1.union(df2), df_list)
        final_dqc_df = reduce(lambda df1, df2: df1.union(df2), df_dqc_list)
        return final_df,final_dqc_df


    #def send_monitor_mail(self, sender: str, receivers: list, thhold_list: list):
    def send_monitor_mail(self,thhold_list: list,thhold_flag: str):
        email_sender = EmailSender(self.sender)
        flag = thhold_flag
        style = r"""<style>
                    table { border-collapse: collapse;  }
                    th, td { padding: 5px; border: solid 1px #777; }
                    th { background-color: DarkBlue; color: White; }
                    </style>"""
        if(flag == "mrm"):
            html = f"""<html>
                        <head></head>
                        {style}
                        <body>
                        <br \>
                        <p>
                        <b>"Number of variables with respect to the Threshold mrm exceeds the limit"</b>
                        </p>
                        <p>
                                List of variables exceeding Threshold mrm: <br \> <br \>
                                {thhold_list}
                        </p>
                        <br \> <br \>  <br \> <br \>  <br \> <br \>
                        <p>
                            <i>Note: This message has been sent by a notification only system. Please do not reply</i>
                        </p>
                        </body>
                        </html> """

        if(flag == "thhold"):
            html = f"""<html>
                        <head></head>
                        {style}
                        <body>
                        <br \>
                        <p>
                        <b>"Number of variables with respect to the Threshold exceeds the limit"</b>
                        </p>
                        <p>
                                List of variables exceeding Threshold: <br \> <br \>
                                {thhold_list}
                        </p>
                        <br \> <br \>  <br \> <br \>  <br \> <br \>
                        <p>
                            <i>Note: This message has been sent by a notification only system. Please do not reply</i>
                        </p>
                        </body>
                        </html> """

        if(flag == "var"):
            html = f"""<html>
                        <head></head>
                        {style}
                        <body>
                        <br \>
                        <p>
                        <b>"Number of variables with respect to the Variation exceeds the limit"</b>
                        </p>
                        <p>
                                List of variables exceeding Threshold Variation: <br \> <br \>
                                {thhold_list}
                        </p>
                        <br \> <br \>  <br \> <br \>  <br \> <br \>
                        <p>
                            <i>Note: This message has been sent by a notification only system. Please do not reply</i>
                        </p>
                        </body>
                        </html> """

        email_sender.send(
            receivers_soeid = self.recivers,
            subject = f"{self.model_name} - Metrics Monitoring Threshold Out of Range",
            html = f"{html}",
            file = None)


    def thhold_variacion(self):
         totales = self.table_total
         current_month = F.current_date()
         last_month = F.date_sub(current_month, 30)

         df_totales_ant = self.spark.sql(f"""SELECT vintage_date, variable, total_psi
                        FROM {totales}
                        where mes(vinatge_date) = last_month""")
         df_actual = self.spark.sql(f"""SELECT vintage_date, variable, total_psi
                        FROM {totales}
                        where mes(vintage_date) = current_month""")

         joined_df = df_totales_ant.alias("last").join(
             df_actual.alias("current"),
             on=["variable"],
             how="inner"
         )

         threshold_percentage = 0.1 # 10%
         df_filtred = joined_df.filter(
             (F.abs((F.col("current.total_psi") - F.col("last.total_psi") / F.col("last.total_psi")) >= threshold_percentage))
         )
         lista_variables = df_filtred.select("variable").distinct().rdd.flatMap(lambda x: x).collect()
         print(current_month)
         print(last_month)
         df_totales_ant.show

         return lista_variables

    #Function that validates the thresholds, in case of not complying with the business rules the process terminates and sends an error message.
    def valida_total_csi_psi(self, final_total_calc_df : DataFrame):
        thhold_list = []
        thhold_mrm_list = []
        thhold_var_list = []
        self.threshold = self.dict_metrics.get("threshold")
        self.treshold_mrm = self.dict_metrics.get("treshold_mrm")
        self.variacion = self.dict_metrics.get("variacion")
        self.threshold_stop = self.dict_metrics.get("threshold_stop")
        self.treshold_mrm_stop = self.dict_metrics.get("treshold_mrm_stop")
        self.variacion = self.dict_metrics.get("variacion_stop")
        #Comparison of total table with threshold values and saving of values in lists
        column_de_values = final_total_calc_df.rdd.map(lambda x:x[2]).collect()
        i=0
        for valor in column_de_values:
              if float(valor) > float(self.threshold):
                 thhold_list.append(final_total_calc_df.collect()[i][1])
              if float(valor) > float(self.treshold_mrm):
                 thhold_mrm_list.append(final_total_calc_df.collect()[i][1])
              i += 1
            # if valor >=  self.variacion:
             #    thhold_var_list.append(valor)
        print("-----------variacion--------------------")
        self.thhold_variacion()
        #If the lists exceeds the number of values allowed per business, the process is terminated
        if len(thhold_mrm_list) >  self.treshold_mrm_stop:
            #self.send_monitor_mail(thhold_mrm_list,"mrm")
            raise Exception(f"""
                    Number of variables with respect to the threshold mrm exceeds the limit:\n
                           Threshold mrm variables list : {thhold_mrm_list} \n\n
                           Threshold variables list: {thhold_list} """)
            self.spark.stop()
            sys.exit(2)
        if len(thhold_list) >  self.threshold_stop:
            #self.send_monitor_mail(thhold_list,"thhold")
            raise Exception(f"""Number of variables with respect to the threshold exceeds the limit:\n
                            {thhold_list} """)
            self.spark.stop()
            sys.exit(2)

    #Function that saves the dataframes to a hive tables
    def to_hive(self, df, table_name: str, temp_table: str):
        logger = logging.getLogger(__name__)
        today = datetime.today().strftime("%Y-%m-%d")
        self.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict").show()
        try:
            df.registerTempTable(temp_table)
            self.spark.sql(f"""INSERT OVERWRITE TABLE {table_name}
                    PARTITION (process_date = '{str(today)}', model_id = {int(self.model_id)})
                    SELECT * FROM {temp_table}""").show()
            logger.info(f"Table {table_name} successfully loaded.")
        except Exception as e:
            logging.error(f"Error uploading table to Hive: \n {e}")
            print("Error al cargar la tabla Hive")

    #Function that rearranges the columns of the dataframes and matches tthe data types requested by the business
    def data_format(self):
        final_df, final_dqc_df = self.psi_csi_calculation("csi")
        final_df = final_df.withColumnRenamed("csi", "psi")
        final_df2, final_dqc_df2 = self.psi_csi_calculation("psi")
        final_df = final_df.union(final_df2)
        final_dqc_df = final_dqc_df.union(final_dqc_df2)

        final_df = final_df.select(final_df.vintage_date,final_df.variable,final_df.bin,final_df.count_dev,\
                                    final_df.dist_dev, final_df.count_current, final_df.dist_current, final_df.psi)

        final_df = final_df.withColumn("vintage_date", col("vintage_date").cast("STRING"))\
                            .withColumn("variable", col("variable").cast("STRING"))\
                            .withColumn("bin", col("bin").cast("Integer"))\
                            .withColumn("count_dev", col("count_dev").cast("BIGINT"))\
                            .withColumn("dist_dev", col("dist_dev").cast("DOUBLE"))\
                            .withColumn("count_current", col("count_current").cast("BIGINT"))\
                            .withColumn("dist_current", col("dist_current").cast("DOUBLE"))\
                            .withColumn("psi", col("psi").cast("DOUBLE"))

        final_total_calc_df = self.total_csi_psi_monthly(final_df,"psi")

        final_total_calc_df = final_total_calc_df.withColumn("vintage_date", col("vintage_date").cast("STRING"))\
                                                .withColumn("variable", col("variable").cast("STRING"))\
                                                .withColumn("total_psi", col("total_psi").cast("DOUBLE"))

        final_dqc_df = final_dqc_df.withColumn("vintage_date", col("vintage_date").cast("STRING"))\
                                    .withColumn("variable", col("variable").cast("STRING"))\
                                    .withColumn("count", col("count").cast("BIGINT"))\
                                    .withColumn("n_miss", col("n_miss").cast("BIGINT"))\
                                    .withColumn("std", col("std").cast("DOUBLE"))\
                                    .withColumn("mean", col("mean").cast("DOUBLE"))\
                                    .withColumn("min", col("min").cast("DOUBLE"))\
                                    .withColumn("p1", col("p1").cast("DOUBLE"))\
                                    .withColumn("p5", col("p5").cast("DOUBLE"))\
                                    .withColumn("p10", col("p10").cast("DOUBLE"))\
                                    .withColumn("p20", col("p20").cast("DOUBLE"))\
                                    .withColumn("p25", col("p25").cast("DOUBLE"))\
                                    .withColumn("p30", col("p30").cast("DOUBLE"))\
                                    .withColumn("p40", col("p40").cast("DOUBLE"))\
                                    .withColumn("p50", col("p50").cast("DOUBLE"))\
                                    .withColumn("p60", col("p60").cast("DOUBLE"))\
                                    .withColumn("p70", col("p70").cast("DOUBLE"))\
                                    .withColumn("p75", col("p75").cast("DOUBLE"))\
                                    .withColumn("p80", col("p80").cast("DOUBLE"))\
                                    .withColumn("p90", col("p90").cast("DOUBLE"))\
                                    .withColumn("p95", col("p95").cast("DOUBLE"))\
                                    .withColumn("p99", col("p99").cast("DOUBLE"))\
                                    .withColumn("max", col("max").cast("DOUBLE"))

        #self.to_hive(final_df,self.table_dist,"bin_table")
        #self.to_hive(final_total_calc_df, self.table_total,"total_table")
        #self.to_hive(final_dqc_df, self.table_dqc,"dqc_table")
        final_total_calc_df.show()
        return final_df,final_total_calc_df,final_dqc_df
