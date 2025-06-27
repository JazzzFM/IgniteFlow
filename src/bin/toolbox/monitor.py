# -*- coding:utf-8 -*-
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import concat, lit
from pyspark.sql import functions as f
from dataclasses import dataclass, field
from typing import NoReturn, Dict, List
from toolbox_temp.merger import PdfMerger
from toolbox_temp.mail import EmailSender
from toolbox_temp.log import LogHandler
from os import getenv, system
import sys

@dataclass
class InputMonitor:
    spark: object
    pivot_date: object
    tables: Dict[str, Dict[str, str]]
    paths: str
    model_name: str = field(init = False)
    out_file: str = field(init = False)
    tmp_plt: str = field(init = False)
    first_v: str = field(init = False)
    data: str = field(init = False)

    def __post_init__(self: object):
        self.model_name = getenv("MODEL_NAME")
        self.out_file = getenv("final_report")
        self.tmp_plt = getenv("TMP_PLOTS")
        self.first_v = getenv("first_view")
        self.data = getenv("data_report")
        

    def last_day_nmonths_before(self: object, num_months: int, dt_format: str) -> str:
        """
        Find the date before the first of the month of `date`.
        For example, if `date` is 2020-12-15, then the date 2020-11-30
        is returned

            :param date: the datetime.date to look at
            :returns: a datetime.date
        """

        day = self.pivot_date.replace(day = 1) - relativedelta(months = num_months)
        day = day - datetime.timedelta(days = 1)
        day_str = day.strftime(dt_format)
        return day_str

    def get_count_history(self: object) -> str:
        self.execution_date = datetime.datetime.today().strftime('%Y-%m-%d')
        self.path = self.paths.get("base_path") + "monitoring_pqt"

        print(f"exec: {self.execution_date}")
        print(f"pivot_date: {self.pivot_date}")

        list_dfs = []
        for k, v in self.tables.items():
            if v.get('lag'):
                lag = int(v.get('lag'))
            else:
                lag = 0

            print(f"lag: {lag}")
            periods = int(v.get('periods')) + lag
            window_start = self.pivot_date.strftime(v.get('dt_format'))
            window_end = self.last_day_nmonths_before(periods, v.get('dt_format'))

            if v.get('periodicity') == 'daily':

                df = self.spark.sql(f"""SELECT
                        {v.get('partition')} as partition,
                        last_day({v.get('partition')}) as end_of_month,
                        COUNT(*) AS count
                    FROM {v.get('name')}
                    GROUP BY {v.get('partition')}
                    ORDER BY {v.get('partition')} DESC""")\
                    .withColumn('year_month', \
                    f.expr('substring(rtrim(partition), 1, 7)'))\
                    .groupBy("year_month", "end_of_month")\
                    .sum("count")\
                    .withColumnRenamed("sum(count)", "count")\
                    .withColumnRenamed("end_of_month", "partition")\
                    .select("partition", "count")\
                    .sort(f.col("partition").desc())\
                    .limit(periods)
                print (f"\n ======= caso daily ======= \n {v.get('name')} \n {df} \n")


            else:

                if f"{k}" == "campanias":
                    df = self.spark.sql(f"""SELECT all_partitions.partition,
                                            COALESCE(id_camp, "CNA") as id_camp,
                                            COALESCE(count, 0) as count
                                            FROM (
                                                SELECT DISTINCT {v.get('partition')} as partition
                                                FROM {v.get('name')}
                                            ) all_partitions
                                            LEFT JOIN (
                                                SELECT
                                                    {v.get('partition')} as partition,
                                                    id_camp AS id_camp,
                                                    COUNT(*) AS count
                                                FROM {v.get('name')}
                                                GROUP BY {v.get('partition')}, id_camp
                                            ) non_empty_partitions
                                            ON all_partitions.partition = non_empty_partitions.partition
                                                WHERE all_partitions.partition <= '{self.pivot_date}'
                                                ORDER BY all_partitions.partition DESC
                                                LIMIT {v.get('periods')}""")
                    df.write.mode("overwrite").parquet(self.path)
                    df = df.drop("id_camp")
                    print (f"\n ======= caso campanias  ======= \n {v.get('name')} \n {df} \n")


                else:
                    if "clocks" in v.get("name"):
                        df = self.spark.sql(f"""
                            SELECT
                                {v.get('partition')}, anio, mes, COUNT(*) AS count
                            FROM {v.get('name')}
                            GROUP BY {v.get('partition')}, anio, mes
                            ORDER BY {v.get('partition')} DESC
                            LIMIT {periods} """)\
                            .withColumn("pre_partition", f.concat(f.col("anio"),\
                                f.lit("-"),f.col("mes"), f.lit("-"), f.lit("01")))\
                            .withColumn("partition", f.last_day(f.col("pre_partition")))\
                            .withColumn("type", f.lit("monthly"))\
                            .select("partition", "count", "type")
                        print (f"\n ======= caso clocks ======= \n {v.get('name')} \n {df} \n")

                    else:

                        df = self.spark.sql(f"""SELECT
                                    {v.get('partition')} as partition,
                                    COUNT(*) AS count
                                    FROM {v.get('name')}
                                    GROUP BY {v.get('partition')}
                                    ORDER BY {v.get('partition')} DESC
                                    LIMIT {periods} """)
                        print (f"\n ======= caso mensual ======= \n {v.get('name')} \n {df} \n")


            # Data Wrangle
            n = df.count()
            if n == 0:
                df_p = pd.DataFrame()
                df_p["partition"] = pd.Series([f"{self.pivot_date}"])
                df_p["count"] = pd.Series([0])

            else:
                df_p = df.toPandas()

            print (f"\n ======= info ======= \n {v.get('name')} \n {df_p} \n")
            list_index = range(len(df_p.index))
            # Add metadata Varibles

            df_p["execution_date"] = pd.Series([f"{self.execution_date}" \
                                                for x in list_index])

            df_p["cohort_date"] =  pd.Series([f"{self.last_day_nmonths_before(lag,'%Y-%m-%d')}" \
                                                for x in list_index])

            df_p["window_start"] = pd.Series([f"{self.last_day_nmonths_before(lag,'%Y-%m-%d')}"\
                                                for x in list_index])

            df_p["window_end"] = pd.Series([f"{self.last_day_nmonths_before(periods, '%Y-%m-%d')}"\
                                                for x in list_index])

            df_p["table_name"] = pd.Series([f"{v.get('name')}" for x in list_index])
            df_p["alias"] = pd.Series([f"{k}" for x in list_index])
            #  Appliying lag to DataFrame
            df_p = df_p.shift(-lag).dropna()
            # Append to a list of DataFrames
            list_dfs.append(df_p)

        df_final = pd.concat(list_dfs, axis = 0)
        print (f"\n======== df_final =========== \n {df_final.to_string(index=False)} \n")
        print(self.data)
        df_final.to_csv(self.data)
        return self.data

    def ggplots_count(self: object) -> str:
        try:
            R_script = "Rscript $BIN_PATH/toolbox_temp/Rplots/ggplotHistory.R"
            R_exec = f"{R_script} '{self.data}' '{self.tmp_plt}' '{self.first_v}' "
            print(R_exec)
            system(R_exec)
            merger = PdfMerger(tmp_path = self.tmp_plt, out_file = self.out_file)

        except Exception as inst:
            print(type(inst))
            print(inst.args)
            print(inst)

        return self.out_file

    def monitor_mail(self: object) -> str:
        file_html_table = open(f"{self.first_v}", mode = 'r', encoding = 'utf-8')
        html_table = file_html_table.read().replace("&lt;", "<").replace("&gt;", ">")
        html = f""" <p>
                         The monitor job at <strong> {self.pivot_date} </strong> are: <br \>
                         {html_table}
                    </p>
                """
        print("\n\n", html, "\n\n")
        file_html_table.close()
        return html
