# -*- coding:utf-8 -*-
from pyspark.sql.functions import col
import pyspark.sql.types as T

from dateutil.relativedelta import relativedelta
from dataclasses import dataclass
from typing import NoReturn, Dict, List
from datetime import datetime

#Custom libraries
from toolbox.merger import PdfMerger
from toolbox.mail import EmailSender
from toolbox.utils import last_day_of_month, last_day_of_n_months_ago
from os import getenv

@dataclass
class InputValidator:
    spark: object
    pivot_date: object
    tables: Dict[str, Dict[str, str]]
    #paths: str
    model_name: str = getenv("MODEL_NAME")

    def get_validation(self: object) -> str:
        #self.path = self.paths.get("base_path") + "monitoring_pqt/"
        dict_noempty = {}
        dict_empty = {}
        boll = 0

        for k, v in self.tables.items():
            

            print(f"Validating partitions of: {v.get('name')}")

            if v.get('lag'):
                lag = int(v.get('lag'))
            else:
                lag = 0

            if v.get('periodicity') == 'daily':
                for period in range(int(v.get('periods'))):
                    aux_date = last_day_of_n_months_ago(self.pivot_date, period)
                    aux_date = aux_date.strftime(v.get('dt_format'))
                    n = self.spark.sql(f"""SELECT {v.get('partition')}
                                        FROM {v.get('name')}
                                        WHERE {v.get('partition')} LIKE '{aux_date}%'""").rdd.count()
                    if n == 0:
                        print(f"\tPartition like {aux_date} does not exists.")
                        if v.get('name') in dict_empty:
                            dict_empty[f"{v.get('name')}"].append([aux_date, f"{n}"])
                        else:
                            dict_empty[f"{v.get('name')}"] = []
                            dict_empty[f"{v.get('name')}"].append([aux_date, f"{n}"])
                    else:
                        print(f"\tPartition like {aux_date} exists.")
                        if v.get('name') in dict_noempty:
                            dict_noempty[f"{v.get('name')}"].append([aux_date, f"{n}"])
                        else:
                            dict_noempty[f"{v.get('name')}"] = []
                            dict_noempty[f"{v.get('name')}"].append([aux_date, f"{n}"])

            elif v.get('periodicity') == 'monthly':
                #Get partitions of interest depending the ingestion date.
                if v.get('ingestion_date') == 'last_d_m': #Last day of the month.
                    base_date = last_day_of_month(self.pivot_date)
                    base_date = last_day_of_n_months_ago(base_date,int(lag)) 
                    #Validate if the partitions exists for every needed month.
                    #First we need check if its a special case for the campanias table
                    df_cam = 0

                    if f"{k}" == "campanias":
                        df_cam = None#self.spark.read.parquet(self.path)
                        count_list = [row["count"] for row in df_cam.select("count").collect()]
                        date_list = [row["partition"] for row in df_cam.select("partition").collect()]
                        camp_list = [row["id_camp"] for row in df_cam.select("id_camp").collect()]
                        print(count_list)
                        print(date_list)

                        num_partitions = len(date_list)

                        if num_partitions != int(v.get('periods')):
                            boll = 1

                        for period in range(len(date_list)):

                            if f"{k}" == "campanias":
                                n = count_list[period]

                                print("\n\n Case campanias n = {n} \n\n")
                                if n == 0:
                                    print(f"\tPartition like {date_list[period]} does not exists.")
                                    if v.get('name') in dict_empty:
                                        dict_empty[f"{v.get('name')}"].append(
                                            [str(date_list[period]) + " - " + camp_list[period], f"{n}"])
                                    else:
                                        dict_empty[f"{v.get('name')}"] = []
                                        dict_empty[f"{v.get('name')}"].append(
                                            [str(date_list[period]) + " - " + camp_list[period], f"{n}"])
                                else:
                                    print(f"\tPartition like {date_list[period]} exists.")
                                    if v.get('name') in dict_noempty:
                                        dict_noempty[f"{v.get('name')}"].append(
                                            [str(date_list[period]) + " - " + camp_list[period], f"{n}"])
                                    else:
                                        dict_noempty[f"{v.get('name')}"] = []
                                        dict_noempty[f"{v.get('name')}"].append(
                                            [str(date_list[period]) + " - " + camp_list[period], f"{n}"])

                    else:

                        T_date = base_date
                        if f"{k}" == "bureau_cohort":
                            T_date = base_date - relativedelta(months=3)
                        for period in range(int(v.get('periods'))):
                            if 'clocks' in v.get('name'):
                                aux_date = first_day_of_n_months_ago(T_date, period)
                            else:
                                aux_date = last_day_of_n_months_ago(T_date, period)
                            aux_date = aux_date.strftime(v.get('dt_format'))

                            n = self.spark.sql(f"""SELECT {v.get('partition')}
                                                FROM {v.get('name')}
                                                WHERE {v.get('partition')} = '{aux_date}'""").rdd.count()
                            if n == 0:
                                print(f"\tPartition like {aux_date} does not exists.")
                                if v.get('name') in dict_empty:
                                    dict_empty[f"{v.get('name')}"].append([aux_date, f"{n}"])
                                else:
                                    dict_empty[f"{v.get('name')}"] = []
                                    dict_empty[f"{v.get('name')}"].append([aux_date, f"{n}"])
                            else:
                                print(f"\tPartition like {aux_date} exists.")
                                if v.get('name') in dict_noempty:
                                    dict_noempty[f"{v.get('name')}"].append([aux_date, f"{n}"])
                                else:
                                    dict_noempty[f"{v.get('name')}"] = []
                                    dict_noempty[f"{v.get('name')}"].append([aux_date, f"{n}"])

                else:
                    # day defined.
                    base_date = self.pivot_date.replace(day=int(v.get('ingestion_date')))
                    #Validate if the partitions exists for every needed month.
                    for period in range(int(v.get('periods'))):
                        aux_date = last_day_of_n_months_ago(base_date, period)
                        aux_date = aux_date.replace(day=int(v.get('ingestion_date')))
                        aux_date = aux_date.strftime(v.get('dt_format'))
                        n = self.spark.sql(f"""SELECT {v.get('partition')}
                                            FROM {v.get('name')}
                                            WHERE {v.get('partition')} = '{aux_date}'""").rdd.count()
                        if n == 0:
                            print(f"\tPartition like {aux_date} does not exists.")
                            if v.get('name') in dict_empty:
                                dict_empty[f"{v.get('name')}"].append([aux_date, f"{n}"])
                            else:
                                dict_empty[f"{v.get('name')}"] = []
                                dict_empty[f"{v.get('name')}"].append([aux_date, f"{n}"])
                        else:
                            print(f"\tPartition like {aux_date} exists.")
                            if v.get('name') in dict_noempty:
                                dict_noempty[f"{v.get('name')}"].append([aux_date, f"{n}"])
                            else:
                                dict_noempty[f"{v.get('name')}"] = []
                                dict_noempty[f"{v.get('name')}"].append([aux_date, f"{n}"])

            elif v.get('periodicity') == 'on_demand':
                #Validate if the table has any partition (catalogs, campains, ...).
                n = self.spark.sql(f"SELECT * FROM {element.get('name')}").rdd.count()
                if n == 0:
                    dict_empty[f"{v.get('name')}"] = "on demand"
                else:
                    dict_noempty[f"{element.get('name')}"] = ['on demand', f"{n}"]

            else: #spetial cases as bimonthly or every 3 years, ...
                if v.get('periodicity')[-1] == 'm': #every x months
                    periodicity = v.get('periodicity').split('_')[0]

        print(f"\n\n{dict_noempty}")
        self.dict_noempty = dict_noempty
        self.dict_empty = dict_empty
        self.boll = boll

    def send_data_quality_mail(self: object, sender: str,
                                receivers: List,
                                html: str,
                                pdf_file: str) -> NoReturn:

        email_sender = EmailSender(sender_soeid = sender)
        list_str_empty = []
        n = 0

        for k,v in self.dict_empty.items():
            list_str_empty.append(
                f"<h5>{k}</h5> <br /><table><tr><th>Partition</th><th>Registers</th></tr>")
            for p in v:
                list_str_empty[n] = f"{list_str_empty[n]} <tr><td>{p[0]}</td><td>{p[1]}</td></tr>"
            list_str_empty[n] = f"{list_str_empty[n]} </table><br />"
            n += 1

        self.list_str_empty = list_str_empty
        list_str_noempty = []
        n = 0

        for k, v in self.dict_noempty.items():
            list_str_noempty.append(
                f"<h5>{k}</h5><br /><table><tr><th>Partition</th><th>Registers</th></tr>")

            for p in v:
                list_str_noempty[n] = f"{list_str_noempty[n]} <tr><td>{p[0]}</td><td>{p[1]}</td></tr>"
            list_str_noempty[n] = f"{list_str_noempty[n]} </table><br />"
            n += 1

        self.list_str_noempty = list_str_noempty

        style = r"""<style>
                    table { border-collapse: collapse;  }
                    th, td { padding: 5px; border: solid 1px #777; }
                    th { background-color: DarkBlue; color: White; }
                    </style>"""

        if self.boll == 0:
            html = f"""<html>
                        <head></head>
                        {style}
                        <body>
                        {html}
                        <p>

                        The execution of validator job has been successfully completed.

                        </p style = ''>
                        <p>
                             <h3>The empty tables partitons are: </h3><br />
                                 {' '.join(self.list_str_empty)} <br />

                        </p>
                        <p>
                             <h3>The non empty tables partitons are: </h3><br />
                                 {' '.join(self.list_str_noempty)} <br />

                        </p>

                        </body>
                        </html> """
        else:
            html = f"""<html>
                        <head></head>
                        {style}
                        <body>
                        {html}
                        <p>

                        The execution of validator job has been successfully completed.

                        </p style = ''>
                        <p>
                             <h3>The empty tables partitons are: </h3><br />
                                 {' '.join(self.list_str_empty)} <br />
                        </p>

                        <p>
                             <h3>The non empty tables partitons are: </h3><br />
                                 {' '.join(self.list_str_noempty)} <br />

                        </p>
                        </body>
                        </html> """

        email_sender.send(
                receivers_soeid = receivers,
                subject = f"{self.model_name} table Validation job is complete",
                html = f"{html}",
                file = pdf_file)

    def make_validation(self: object) -> NoReturn:
        n = len(self.dict_empty)
        if n > 0:
            raise Exception(f"""
                    There are {n} empty table partitions:\n
                            {self.list_str_empty} """)
            self.spark.stop()
            sys.exit(2)
        else:
            print("All tables Success")
