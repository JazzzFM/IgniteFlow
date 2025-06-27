"""
SUMMARY:
    Script that contain the set of abstractions created by CoE Risk
    Model team to execute the Model Logic process through the Machine
    Learning Operation Lifecycle.
"""
# !/usr/bin/env python
# -*- CODING: UTF-8 -*- VERSION: 3.6 -*-


# Python/PySpark Lib
import pandas as pd
from functools import reduce
import pyspark.sql.types as T
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql import functions as F
#from pyspark.sql.functions import max
from pyspark.sql import HiveContext
from datetime import datetime
from cytoolz.functoolz import compose
import random
import calendar
import os
import subprocess
import re
import traceback
from dateutil.relativedelta import relativedelta
from dataclasses import dataclass, field
import unidecode
import time

# Neural Flow Framework Lib
from toolbox_temp.write import Writer
from toolbox_temp.read import InReader
from lib_log import LogHandler

#from bnmxverse.missing_treatment import MissingTreatment
#from bnmxverse.extract import Extract
#from bnmxverse.group_by import GroupBy
#from bnmxverse.generate_pivot import GeneratePivot
#from bnmxverse.generate_ratios import GenerateRatios
#from bnmxverse.feature_engineering import FeatureEngineering
#from bnmxverse.vector_assembler import FeaturesVectorAssembler
#from bnmxverse.ttd_processed import TTDProcessed
#from bnmxverse.email_sender import EmailSender

@dataclass
class Kernel:
    spark: object
    config: object
    date: object
    config_custom: object = field(init = False)
    sc: object = field(init = False)
    sqlContext: object = field(init = False)
    writer: object = field(init = False)
    reader: object = field(init = False)
    Log: object = field(init = False)
    logger: object = field(init = False)
    email_sender: object = field(init = False)

    def __post_init__(self: object):

        if hasattr(self.config, 'config_custom'):
            self.config_custom = self.config.config_custom

        if hasattr(self.config, 'config_rigid'):
            self.config_rigid = self.config.config_rigid

        self.sc = self.spark.sparkContext
        self.sqlContext = HiveContext(self.spark)
        self.writer = Writer(self.spark, self.sc)
        self.reader = InReader(self.spark, config = self.config)
        self.Log = LogHandler()
        self.logger = self.Log.logger


        if hasattr(self.config, 'config_custom') and hasattr(self.config, 'config_rigid'):
            self.sources = self._get_sources()
            self.pivot = self._pivot_preparation(self.sources)
            self._quita_acentos()
            self._change_types_config()
            self.email_sender = EmailSender(self.config.monitor_validator.get("email").get("sender"))

@dataclass
class KernelDataPull(Kernel):
    """
    Main class of datapull process of COE team
    Parameters
    ----------
    :config_rigid: dictionary
        Dictionary with all configurations setting by default by the data engineer team of COE Advance Analytics
    :config_custom: dictionary
        Dictionary with all custom settings defined by the final users
    :sqlContext: spark context
        sqlContext defined in the script
    """
    message_start_controls_stract = """<head>
                                        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
                                        <h1> Input controls: </h1>"""
    message_start_finals = """<head>
                            <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
                            <h1> Times of execution: </h1>"""

    message_style = """<style type="text/css" media="screen">
                    #default {
                        font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
                        font-size: 12px;
                        border: 3px solid #FFCD00 ;
                    }

                     #default th {
                        padding-top: 4px;
                        padding-bottom: 4px;
                        text-align: center;
                        background-color: #002D72;
                        color: white;
                    }

                    #default tbody td {
                        border: 1px solid #FFFFFF;
                    }

                    #default th {
                        border: 1px solid #FFFFFF;
                    }

                    #default td, #default th {
                        border: 1px solid #ddd;
                        padding: 4px;
                    }

                    #default tr {background-color: #eaeaea;
                        text-align: center;
                    }

                    </style>
                    </head>
                    <body>"""

    message_end  = """</body>"""
    subject_controls = "Input controls for source {}"
    subject_source_completed = "Datapull process for source {}"

    def _change_types_config(self):
        """
        Change types of the custom config dictionary
        """
        print(f"config_custom: {self.config_custom} \n\n")

        for key in ["n_mis","period","n_threads"]:
            self.config_custom[key] = int(self.config_custom[key])

    def _quita_acentos(self):
        """
        Define the udf for removing special characters.
        """
        def quita_acentos(unicodestring):
            if unicodestring == None:
                return ""
            return unidecode.unidecode(unicodestring).lower()
        self.sqlContext.udf.register("quita_acentos", quita_acentos)

    def _get_sources(self):
        """
        Identify if the sources from the custom settings is a list of a string and return a list
        :Returns:
        -------
        list
            All the sources for the execution defined by the final user
        """

        if type(self.config_custom["sources"]) is list:
            return(self.config_custom["sources"])
        else:
            return([self.config_custom["sources"]] )

    def run(self, source):
        """
        Perform the process of datapull
        """
        # for source in self.sources:
        try:
            self._pipeline(source)
        except Exception as e:
            traceback_output = traceback.format_exc()
            mess_error = f"""An error was found in source {source},\n
                                contact data engineer team for details and solutions"""
            print(mess_error)
            print(traceback_output)
            subject = "Error in execution of source {}".format(source)

            self.email_sender.send(
                    self.config_custom["email_receivers"],
                    f'{subject}',
                    mess_error + f"\n{traceback_output}")

    def _pipeline(self, source):
        """
        Create and perform the pipeline for a specified source
        Parameters
        ----------
        :source: string
            Name of source for making the datapull process
        """
        print(f"Start datapull for source {source}")

        ########### Define general constants ###########
        self.source = source
        self.env = self.config.env
        source_dict = self.config_rigid[source]
        write_path = os.path.join(self.config_custom["working_path"], self.config.process_date, source)
        number_threads = self.config_custom["n_threads"]
        write_mode = self.config_custom["write_mode"]

        print(f"write_path: {write_path}")
        print(f"write_mode: {write_mode}")

        ################# DESCOMENTAR DESPUES
        steps_list = self.config_rigid[source]["steps_list"]
        year_id = self.config.process_date

        ########### Execution of different steps ###########
        previous_step = None
        previous_path = None

        #Defining missing treatment (required because contains missing treatment and mobs in the same class)
        # mt=missing_treatment(year_id,source_dict,source,self.config_custom["n_mis"])
        mt = MissingTreatment(year_id, source_dict, source, self.config_custom["n_mis"], env = self.config.env)
        time_execution = []

        print(f"\n\n steps_list: {steps_list} \n\n")

        for step in steps_list:
            time_start = time.time()

            if step == "extract":
                self._perform_extract(source_dict, write_path, number_threads, write_mode, source)
                previous_step = "extract"
                previous_path = os.path.join(write_path,"extract")

                print(f"previous_step: {previous_step}")
                print(f"previous_path: {previous_path}")

            elif step == "mt":
                path_mt = previous_path
                tables_mt = [table + "_historic" for table in source_dict["extract"]["table_name"]]
                self._perform_mt(mt, path_mt, tables_mt, self.config_custom["working_path"])
                previous_step = "missing_treatment"
                previous_path = os.path.join(write_path,"missing_treatment")
                print(f"previous_step: {previous_step}")
                print(f"previous_path: {previous_path}")

            elif step == "groupby":
                path_groupby = os.path.join(write_path,"groupby")
                agg_dict = source_dict["group_by"]
                self._perform_groupby(path_groupby, source, previous_path, agg_dict)
                previous_step = "groupby"
                previous_path = os.path.join(write_path,"groupby")
                print(f"previous_step: {previous_step}")
                print(f"previous_path: {previous_path}")

            elif step == "pivoting":
                path_pivoting = os.path.join(write_path, "pivot")
                pivot_dict = source_dict["pivoting"]
                self._perform_pivoting(path_pivoting, previous_path, pivot_dict)
                previous_step = "pivot"
                previous_path = os.path.join(write_path, "pivot")
                print(f"previous_step: {previous_step}")
                print(f"previous_path: {previous_path}")

            if step == "ratios":
                print("Performing ratios")
                ratios_dict = source_dict["ratios"]
                print(f"previous_path: {previous_path}")
                ratios_dict["time_window"] = self._get_time_window(self.config_custom["n_mis"],
                                                                self.config_custom["period"])
                ratios = GenerateRatios(previous_path,
                                        ratios_dict,
                                        source,
                                        self.sqlContext,
                                        write_mode)
                ratios.run()
                previous_step = "ratios"
                if source == "corporate":
                    previous_path = os.path.join(write_path,"features")
                else:
                    previous_path = os.path.join(write_path,"ratios")
                print("Ending ratios")
                print(f"previous_step: {previous_step}")
                print(f"previous_path: {previous_path}")


            elif step == "fe":
                df_transpose = self.sqlContext.read.parquet(previous_path)
                self._perform_fe(df_transpose, write_path)
                previous_step = "features"
                previous_path = os.path.join(write_path,"features")
                print(f"previous_step: {previous_step}")
                print(f"previous_path: {previous_path}")

            if step == "mobs":
                self._create_mobs_extract()
                working_path = self.config_custom["working_path"]
                path_fe = previous_path
                previous_path = os.path.join(write_path,"mobs")
                self._perform_mobs(working_path, path_fe, mt, previous_path)
                previous_step = "mobs"
                previous_path = os.path.join(write_path,"mobs","imputate_mobs")
                print(f"previous_step: {previous_step}")
                print(f"previous_path: {previous_path}")

            elif step == "va":
                previous_step = "va"
                path_features = previous_path
                self._perform_va(path_features = path_features, write_path = write_path)
                print(f"previous_step: {previous_step}")
                print(f"previous_path: {previous_path}")

            time_end = time.time()
            time_execution.append(self._calculate_time(time_end-time_start))

        execution_dataframe = pd.DataFrame({"steps" : steps_list, "time of execution": time_execution})
        message_start = self.message_start_finals
        message_style = self.message_style
        message_end = self.message_end
        html_mess = self._create_html_notification_times(execution_dataframe,
                                                        message_start,
                                                        message_style,
                                                        message_end)

        subject = self.subject_source_completed.format(source)

        self.email_sender.send(
            self.config_custom["email_receivers"],
            f'{subject} completed',
            html_mess)

        print(f"Proces Data Pull {source} ends")

    def _calculate_time(self, time_passed):
        """
        Calculates and gives format of minutes, hours and seconds for a certain time in seconds given
        Parameters
        ----------
        :time_passed: float
            Time passes, is the time to convert and give format
        :Returns:
        -------
        string
            The time of that transformation
        """
        time_passed = round(time_passed)
        if time_passed / 3600 > 1:
            hours = time_passed // 3600
            minutes = (time_passed - hours * 3600) // 60
            return f"{hours} hours, {minutes} minutes"
        elif time_passed / 60 > 1:
            minutes = time_passed // 60
            seconds = time_passed - minutes * 60
            return f"{minutes} minutes, {seconds} seconds"
        else:
            return f"{time_passed} seconds"

    #########################################################
    ############# Perform of steps###########################
    #########################################################

    def _perform_extract(self, source_dict, write_path, number_threads, write_mode, source):
        """
        Perform extract
        Parameters
        ----------
        :source_dict: string
            Dictionary of the source
        :write_path: string
            Path to write the extract
        :number_threads:
            Number of threads to use in execution
        :write_mode:
            Mode of write of parquets
        :source:
            Source to perform the extract
        """
        print("Starting extract")
        sources_cat = source_dict["extract"]
        path_extract = os.path.join(write_path, "extract")
        if self._check_extract(path_extract = path_extract, extract_dictionary = sources_cat):
            n_mis = self.config_custom["n_mis"]
            extract = Extract(pivot_table = self.pivot, working_path = write_path, n_mis = n_mis,\
                            sources_cat = sources_cat, sqlContext = self.sqlContext,\
                            number_threads = number_threads, mode = write_mode)
            extract.collect_history()

            html_mess = extract.create_html_controls(self.message_start_controls_stract,
                                                    self.message_style, self.message_end)
            subject = self.subject_controls.format(source)
            self.email_sender.send(
                self.config_custom["email_receivers"],
                '{} completed'.format(subject),
                html_mess)

            print("Ending extract")
        else:
            print(f"Extract was already performed for source {source}!")

    def _perform_mt(self, mt, path_mt, tables_mt, path_write):
        """
        Perform missing treatment
        Parameters
        ----------
        :mt: object of MissingTreatment class
            The mt object of the source
        :path_mt: string
            Path where the dataframes with missings are saved
        :tables_mt: list
            List with all the tables to perform missing
        :path_write: string
            Correspond to working path
        """
        print("Starting missing treatment")
        path_write = self.config_custom["working_path"]
        path_missing = os.path.join(path_mt[:path_mt.rindex("/")],"missing_treatment")

        print(f"path_missing: {path_missing}")
        if self._check_missing(path_missing = path_missing, tables_missing = tables_mt):
            print(f"path_write: {path_write}")
            mt.run_mt(path_mt, tables_mt, path_write, self.sqlContext)
            print("Ending missing treatment")
        else:
            print("Missing treatment was already performed!")

    def _perform_groupby(self, path_groupby, source, previous_path, agg_dict):
        """
        Perform groupby
        Parameters
        ----------
        :path_groupby: string
            Path where the groupby is saved
        :source: string
            Name of source
        :previous_path:
            Path of the previous step
        :agg_dict:
            Dictionary of groupby
        """
        if self._check_groupby(path_groupby):
            group_by = GroupBy(source, previous_path, agg_dict, self.sqlContext)
            group_by.run()
        else:
            print("Groupby was already performed!")

    def _perform_pivoting(self, path_pivoting, previous_path, pivot_dict):
        """
        Perform pivoting
        Parameters
        ----------
        :path_pivoting: string
            Path where the pivoting is saved
        """
        print("Starting pivoting")
        if self._check_pivoting(path_pivoting):
            pivoting = GeneratePivot(previous_path, pivot_dict, self.sqlContext)
            pivoting.pivot_data()
        else:
            print("pivoting was already performed!")
        print("Ending pivoting")

    def _perform_fe(self, df_transpose, write_path):
        """
        Perform feature engineering
        Parameters
        ----------
        :df_transpose: pyspark dataframe
            Output of transpose process in datapull
        :write_path: string
            Path to write the output of feature engineering
        """
        ########### Constants of feature engineering ###########
        trans_list = self.config_custom["lista_trans"]
        path_fe = os.path.join(write_path, "features")
        period = self.config_custom["period"]
        number_threads = self.config_custom["n_threads"]
        write_mode = self.config_custom["write_mode"]
        base_path = self.config_custom["base_path"]

        ########### Check if Feature Engineering was already performed ###########
        print("df_transpose --\n")

        fe = FeatureEngineering(df = df_transpose,
                                working_path = write_path,
                                period = period,
                                sqlContext = self.sqlContext,
                                env = self.config.env,
                                base_path = base_path,
                                source = self.source,
                                number_threads = number_threads,
                                mode = write_mode)

        if not self._evaluate_path(path_fe):
            fe.run(trans_list)
        else:
            list_remaining = self._fe_performed(path_fe, trans_list)
            if list_remaining:
                fe.run(list_remaining)
            else:
                print("Feature engineering was already performed!")

    def _perform_mobs(self, working_path, path_fe, mt, path_mobs):
        print("Performing mobs!")
        print(f"path_fe = {path_fe}")
        files_fe = self._check_files(path_fe)
        periodo = self.config_custom["period"]
        path_mobs_aux = os.path.join(path_mobs,"imputate_mobs")
        print("into run_mobs")
        print(f"path_mobs_aux = {path_mobs_aux}")

        if self._check_mobs(path_mobs_aux, files_fe):
            feat_list = self._get_mobs_list(path_fe)
            print(f"feat_list = {feat_list}")
            mt.run_mobs(working_path,
                        path_mobs,
                        path_fe,
                        feat_list,
                        periodo,
                        self.sqlContext)
        else:
            print("Mobs already performed!")
        print("Ending mobs!")

    def _perform_va(self, path_features, write_path):
        """
        Perform vector assembler
        Parameters
        ----------
        :path_features: string
            Path where the features are located
        :write_path: string
            Path to write the vector assembler
        """
        print("Starting vector assembler")
        save_path = os.path.join(write_path,"vector_assembler")

        if self._evaluate_path(save_path):
            print(f"Vector assembler was already performed")
        else:
            va = FeaturesVectorAssembler(path_features = path_features,
                                        save_path = save_path,
                                        sqlContext = self.sqlContext,
                                        env = self.env)
            va.run()
            print("Ending vector assembler")

    #########################################################
    ############# Check the current step#####################
    #########################################################

    def _check_extract(self, path_extract, extract_dictionary):
        """
        Check if extract was previously executed in the workpath
        Parameters
        ----------
        :path_extract: string
            path of extract
        :extract_dictionary: dict
            Dictionary of extract for a given source
        :Returns:
        -------
        Boolean
            True if extract need to be performed and False if already exist (for all tables)
        """
        tables_historic = [table + "_historic" for table in extract_dictionary["table_name"]]
        files = self._check_files(path_extract)
        if files:
            for file_aux in files:
                if file_aux in tables_historic:
                    tables_historic.remove(file_aux)
            if tables_historic:
                return True
            else:
                return False
        else:
            return True

    def _check_missing(self, path_missing, tables_missing):
        """
        Check if missing treatment was previously executed in the workpath
        Parameters
        ----------
        :path_missing: string
            path of extract
        :tables_missing: list
            List of the missing tables of output
        :Returns:
        -------
        Boolean
            True if extract need to be performed and False if already exist (for all tables)
        """
        tables_aux = tables_missing.copy()
        files = self._check_files(path_missing)
        if files:
            for file_aux in files:
                if file_aux in tables_aux:
                    tables_aux.remove(file_aux)
            if tables_aux:
                return True
            else:
                return False
        else:
            return True

    def _check_groupby(self,path_groupby):
        """
        Check if group by was previously executed in the workpath
        Parameters
        ----------
        :path_groupby: string
            path of group by
        :Returns:
        -------
        Boolean
            True if group by need to be performed and False if already exist (for all tables)
        """
        if self._evaluate_path(path_groupby):
            return False
        else:
            return True

    def _check_pivoting(self,path_pivoting):
        """
        Check if pivoting was previously executed in the workpath
        Parameters
        ----------
        :path_pivoting: string
            path of pivoting
        :Returns:
        -------
        Boolean
            True if pivoting need to be performed and False if already exist (for all tables)
        """
        if self._evaluate_path(path_pivoting):
            return False
        else:
            return True

    def _fe_performed(self, path_fe, trans_list):
        """
        Check what transformations of feature engineering was already performed and
            returns the transformations that was not performed.

        Parameters
        ----------
        :path_fe: string
            path of fe
        :trans_list: list
            List of transformations
        :Returns:
        -------
        list
            Transformations that was already performed
        """
        trans_list_aux = trans_list.copy()
        files = self._check_files(path_fe)
        for fil in files:
            if fil[:fil.index("_")] in trans_list_aux:
                trans_list_aux.remove(fil[:fil.index("_")])

        # The condition below is to check if another transformation
        # excluding ratios mod was not performed

        if len(trans_list_aux) > 1:
            return trans_list_aux
        else:
            if len(files)-len(trans_list) <= 8:
                return []
            else:
                return ["ratiosmod"]

    def _check_mobs(self, path_mobs, files_fe):
        """
        Check if mobs was previously performed
        ----------
        :path_mobs: string
            path of extract
        :files_fe: list
            Files in feature engineering
        :Returns:
        -------
        Boolean
            True if extract need to be performed and False if already exist (for all tables)
        """
        files = self._check_files(path_mobs)
        if files:
            for file_aux in files:
                if file_aux in files_fe:
                    files_fe.remove(file_aux)
            if files_fe:
                return True
            else:
                return False
        else:
            return True

    #########################################################
    ############# Preparation of steps#######################
    #########################################################

    def _pivot_preparation(self, sources):
        """
        Preparates the pivot, it takes the normal pivot and return a pivot with multiple
        liabilities and credit cards accounts for specified sources.
        Parameters
        ----------
        :sources: list
            List of all sources to see if preparation is needed.
        :Returns:
        -------
        pyspark dataframe
            The new pivot preparated
        """
        print("Preparing pivot for the sources")
        path_new_pivot = os.path.join(self.config_custom["working_path"],
                            self.config.process_date,
                            "pivot_prepared")

        if self._evaluate_path(path_new_pivot):
            print("Pivot already prepared!")
            return self.sqlContext.read.parquet(path_new_pivot)

        self.pivot_path = self.config_custom["pivot_path"] + str(self.config.process_date) + "/"

        print(f"\n pivot_path: {self.pivot_path} \n")
        pivot = self.sqlContext.read.parquet(self.pivot_path)
        vintages = self._get_vintages_dates(pivot)
        n_mis = self.config_custom["n_mis"]

        if self._check_crd_acct_nbr(sources):

            print("primer caso")
            lag = 2
            max_date, min_date = self._get_range_dates(vintages, lag, n_mis)

            df_ecm = self.sqlContext.sql(self.config_rigid["query_ecm"])
            print(f"query_ecm: {self.config_rigid['query_ecm']} \n")
            print(f"max_date: {max_date}\n")
            print(f"min_date: {min_date} \n")

            df_ecm = df_ecm.where(df_ecm.proc_dt < max_date)\
                            .where(df_ecm.proc_dt >= min_date)[["numcliente","crd_acct_nbr"]]\
                            .distinct()
            pivot = pivot.join(df_ecm, pivot.numcliente == df_ecm.numcliente, how="left")\
                            .drop(df_ecm.numcliente)

        if self._check_contratos(sources):
            print("segundo caso")
            lag = 0
            max_date, min_date = self._get_range_dates(vintages, lag, n_mis)

            for query in self.config_rigid["queries_liabilities"]:
                df_liab = self.sqlContext.sql(query)

                if "id_cuenta16" in df_liab.columns:
                    df_liab = df_liab.where(df_liab.mis_date < max_date)\
                                    .where(df_liab.mis_date >= min_date)[["numcliente","id_cuenta16"]]\
                                    .distinct()
                else:
                    columnas = df_liab.columns
                    columnas.remove("mis_date")
                    df_liab = df_liab.where(df_liab.mis_date < max_date)\
                                .where(df_liab.mis_date >= min_date)[columnas]\
                                .distinct()

                pivot = pivot.join(df_liab,pivot.numcliente == df_liab.numcliente,how="left").drop(df_liab.numcliente)

        if self._check_crd_acct_nbr(sources) and self._check_contratos(sources):
            print("tercer caso")
            cols_pivot = ['numcliente', 'vintage', 'crd_acct_nbr',
                            'num_contrato_maestra', 'num_contrato_perfiles',
                            'num_contrato_perf_ejec', 'num_contrato_cheques',
                            'num_contrato_otros', 'id_cuenta16']
            pivot = pivot[cols_pivot].distinct()

        elif self._check_crd_acct_nbr(sources):
            print("cuarto caso")
            cols_pivot = ['numcliente', 'vintage', 'crd_acct_nbr']
            pivot = pivot[cols_pivot].distinct()

        elif self._check_contratos(sources):
            print("quinto caso")
            cols_pivot = ['numcliente', 'vintage','num_contrato_maestra',
                            'num_contrato_perfiles', 'num_contrato_perf_ejec',
                            'num_contrato_cheques', 'num_contrato_otros',
                            'id_cuenta16']
            pivot = pivot[cols_pivot].distinct()

        else:
            return pivot

        pivot.write.mode("overwrite").parquet(path_new_pivot)
        pivot = self.sqlContext.read.parquet(path_new_pivot)
        print(f"path_new_pivot: {path_new_pivot}")
        print("Ending pivot preparation")
        return pivot

    def _create_mobs_extract(self):
        """
        Creates an extraction for mobs module
        Parameters
        ----------
        :time_passed: float
            Time passes, is the time to convert and give format
        :Returns:
        -------
        string
            The time of that transformation
        """
        print("Creating mobs preparation")
        sources_cat = self.config_rigid["mobs_preparation"]
        write_path = os.path.join(self.config_custom["working_path"], self.config.process_date)
        n_mis = self.config_custom["n_mis"]
        path_premobs = os.path.join(write_path,"mobs_preparation")
        print(f"--------> path_premobs =  {path_premobs}")
        print(f"--------> INTO _create_mobs_extract ")
        if self._check_mobs_extract(path_premobs):
            extract = Extract(pivot_table = self.pivot, working_path = write_path, n_mis = n_mis,\
                            sources_cat = sources_cat, sqlContext = self.sqlContext, name = "mobs_preparation")
            extract.collect_history()
        else:
            print("Premobs already performed!")
        print("Ending mobs preparation")

    def _check_mobs_extract(self, path_premobs):
        """
        Check if premobs was already performed
        Parameters
        ----------
        :path_premobs: string
            Path where premobs is located
        :Returns:
        -------
        boolean
            True if need to be performed false if not
        """
        length = len(self._check_files(path_premobs))
        print(length)
        if length == 8:
            return False
        else:
            return True

    def _check_crd_acct_nbr(self, sources):
        """
        Check if some source need the credit account number
        Parameters
        ----------
        :sources: list
            List of sources to check if need the credit account number
        :Returns:
        -------
        boolean
            True if some source need false if not
        """
        for source in sources:
            if source in self.config_rigid["need_crd_acct_nbr"]:
                return True
        return False

    def _check_contratos(self,sources):
        """
        Check if some source need the "contrato" number
        Parameters
        ----------
        :sources: list
            List of sources to check if need the "contrato" number
        :Returns:
        -------
        boolean
            True if some source need false if not
        """
        for source in sources:
            if source in self.config_rigid["need_contratos"]:
                return True
        return False

    def _get_mobs_list(self, path_fe):
        """
        Creates two list, the first contains all the transformations that need a mobs treatment and the second
        contains the transformations that just are copied
        Parameters
        ----------
        :path_fe: string
            Path where the transformations were saved
        :Returns:
        -------
        tuple
            Tuple containing (transformations need imputation, transfromations just need be copied)
        """
        files_fe = self._check_files(path_fe)
        list_no_mobs = self.config_rigid["list_no_mobs"]
        list_copy = []
        list_imputate = []
        for feature_transformation in files_fe:
            if feature_transformation in list_no_mobs:
                list_copy.append(feature_transformation)
            else:
                list_imputate.append(feature_transformation)
        return list_imputate, list_copy

    def _get_vintages_dates(self,pivot):
        """
        Get the distinct vintages dates from pivot

        Parameters
        ----------
        :pivot: pyspark DataFrame
            Dataframe containing the customers we want the information and the vintage date of that customer
        :Returns:
        -------
        List
            All the vintages dates in pivot
        """
        pivot.createOrReplaceTempView("pivot_temporal")
        vintages = self.sqlContext.sql("select distinct vintage from pivot_temporal").collect()
        vintages = [vintages[p][0] for p in range(len(vintages))]
        vintages.sort(reverse = True)
        return vintages

    def _get_range_dates(self, vintages, lag, n_mis, strftime = "%Y-%m-%d"):
        """
        Get the range of dates to get customers list in one table

        Parameters
        ----------
        :vintages: list
            list with all vintages dates in pivot
        :lag: int
            number of months of lag in the source
        :vintages: pyspark DataFrame
            number of months of information we want
        :strftime: string
            format to display the date to string, by default "%Y-%m-%d"
        :n_mis:
        -------
        Tuple
            Tuple in form (max_date,min_date)
        """

        max_date = max(vintages) + relativedelta(days = 1) - relativedelta(months = - 1, days = 1)
        min_date = min(vintages) + relativedelta(days = 1) - relativedelta(months = n_mis + lag - 1, days = 1)
        return (max_date.strftime(strftime),min_date.strftime(strftime))

    def _get_time_window(self,n_mis,period):
        """
        Get a list with the grouped months of information in feature engineering
        Parameters
        ----------
        :n_mis: int
            Number of months of information
        :period: int
            Number of grouped months in fe
        :Returns:
        ---------
        list
            List with the amount of grouped months, example: [3,6,9,12]
        """
        time_window = []
        for k in range(1,n_mis+1):
            if k % period == 0:
                time_window.append(period * (k // period))
        return time_window

    def _obtain_path(self, path, substring):
        """
        Obtain the path that contain a certain substring
        Parameters
        ----------
        :path: string
            Obtain the path that contains a certain substring
        :substring: string
            Substring to obtain path
        :Returns:
        -------
        string
            The path that contains subscript
        """
        files = self._check_files(path)
        for string in string_list:
            if substring in string:
                return os.path.join(path,string)

    def _check_substring(self, string_list, substring):
        """
        Check if a substring is contained in a list of strings
        Parameters
        ----------
        :string_list: list
            List with strings to check if contain the substring
        :substring: string
            String to check if is contained in one string of the list
        :Returns:
        -------
        Boolean
            True if substring is in a string of list, False if not
        """
        for string in string_list:
            if substring in string:
                return True
        return False

    def _check_files(self,path):
        """
        Obtain a list with the files of certain path in hadoop.
        Parameters
        ----------
        :path: string
            path to obtain the files
        :Returns:
        -------
        List
            List with all the files in the path
        """
        args_list=['hdfs', 'dfs', '-ls', path]
        print(f"path: {path}")
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        proc.wait()


        if self.env == "UAT" or self.env == "PROD":
            len_str = 69
        else:
            len_str = 53

        list_files = []
        for elem in stdout.decode('ascii','ignore').splitlines():
            if len(elem[len_str:])>0:
                print(f"elem = {elem}")
                list_files.append(re.sub(path + "/", '', str(elem[len_str:])))

        for file_ in list_files:
            print(f"\n {file_}")

        return list_files

    def _evaluate_path(self, path):
        """
        Evaluate if certain path exists in hadoop
        Parameters
        ----------
        :path: string
            path to check if exists
        :Returns:
        -------
        Boolean
            True if path exist false if not
        """
        args_list = ['hdfs', 'dfs', '-test', '-d', path]
        proc = subprocess.Popen(args_list,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

        proc.communicate()
        value = proc.returncode==0 and True or False
        return value

    def _create_html_notification_times(self, table_times, message_start,
                                        message_style, message_end,
                                        table_id = "default"):
        """
        Create html with the pandas for sending an email with the controls

        Parameters
        ----------
        :table_times: pandas dataframe
            Dataframe with time of every step
        :message_start: string
            Innitial comment for title in the email
        :message_syle: string
            Should contain the style you want to use for tables
        :message_end: string
            Contains the final comment and the <\body>
        :table_id: string
            Style in message_style you want to use, it has default value
        Returns
        -------
        String
            Contains the full html code for sending an email
        """
        mess_final = message_start + message_style +\
                        f"""<br> Below are showed the time of execution of every step:
                        <br> {table_times.to_html(index=False,table_id = table_id)} <br>""" +\
                        message_end
        return mess_final
