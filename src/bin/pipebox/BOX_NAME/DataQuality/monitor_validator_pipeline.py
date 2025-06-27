# !/usr/bin/env python
# -*- coding: utf-8 -*- version: 3.7 -*-
from toolbox.monitor import InputMonitor
from toolbox.validator import InputValidator
from toolbox.kernel import Kernel
from toolbox.log import LogHandler
from dataclasses import dataclass
from typing import NoReturn
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pandas as pd
import inspect
import os
import subprocess, sys
import collections
import calendar
import datetime
import logging

@dataclass
class DataQuality(LogHandler, Kernel):

    @LogHandler.logger_dec
    def get_date(self) -> datetime:
        """
        Si el archivo de configuración tiene fecha vacía se
        toma el día de hoy como referencia y vemos un mes
        atrás el último día

        Si la fecha está, debería ser un añomes, hay que convertirlo
        a datetime tomando el primer día del mes, y haciendo el
        mismo tratamiento
        """
        self.process_date = str(self.config.process_date)
        print(f"process_date: {self.process_date}")
        if self.process_date is None:
            today = datetime.date.today().replace(day = 1)
        else:
            year = int(self.process_date[:4])
            month = int(self.process_date[-2:])
            today = datetime.date(year, month, 1).replace(day = 1)

        real_date = today - datetime.timedelta(days = 1)
        return real_date


    @LogHandler.logger_dec
    def parameters(self: object) -> NoReturn:
        self.log_info("DataQuality -- parameters -- main start")

        self.date = self.get_date()
        self.dict_tables = self.config.monitor_validator.get("tables")
        self.catalog = [self.config.monitor_validator.get("tables").get("campanias")]
        self.paths = self.config.tables
        self.sender = self.config.monitor_validator.get("email").get("sender")
        self.receivers = self.config.monitor_validator.get("email").get("receivers")

        print(f"{self.date.strftime('%Y-%m-%d')} \n")
        print(f"dict_tables: {self.dict_tables} \n")
        print(f"catalog: {self.catalog} \n")
        print(f"paths: {self.paths} \n")
        print(f"sender: {self.sender} \n")
        print(f"receivers: {self.receivers} \n")

        self.monitor = InputMonitor(spark = self.spark,
                                    pivot_date = self.date,
                                    tables = self.dict_tables,
                                    paths = self.paths)

        self.validator = InputValidator(spark = self.spark,
                                        pivot_date = self.date,
                                        tables = self.dict_tables,
                                        paths = self.paths)

        self.log_info("DataQuality -- parameters -- ends")

    @LogHandler.logger_dec
    def monitoring_RScript(self: object):
        self.log_info("DataQuality -- monitoring_RScript -- starts")
        self.path_csv = self.monitor.get_count_history()
        self.log_info("DataQuality -- monitoring_RScript -- ends")

    @LogHandler.logger_dec
    def create_ggplots(self: object):
        self.log_info("DataQuality -- ggplots_count -- starts")
        self.final_report = self.monitor.ggplots_count()
        self.log_info("DataQuality -- ggplots_count -- ends")

    @LogHandler.logger_dec
    def create_mail(self: object):
        self.log_info("DataQuality -- monitor_mail -- starts")
        self.html_body_monitor = self.monitor.monitor_mail()
        self.log_info("DataQuality -- monitor_mail -- ends")

    @LogHandler.logger_dec
    def validation(self: object):
        self.log_info("DataQuality -- get_validation -- starts")
        self.validation = self.validator.get_validation()
        self.log_info("DataQuality -- get_validation -- ends")

    @LogHandler.logger_dec
    def send_mail(self: object):
        self.log_info("DataQuality -- send_mail -- starts")
        self.validator.send_data_quality_mail(sender = self.sender,
                                             receivers = self.receivers,
                                             html = self.html_body_monitor,
                                             pdf_file = self.final_report)
        self.log_info("DataQuality -- send_mail -- ends")

    @LogHandler.logger_dec
    def chain_success(self: object):
        self.log_info("DataQuality -- chain success -- starts")
        self.validator.make_validation()
        self.log_info("DataQuality -- chain success -- ends")

    @LogHandler.logger_dec
    def run(self: object):
        self.parameters()
        self.monitoring_RScript()
        self.create_ggplots()
        self.create_mail()
        self.validation()
        self.send_mail()
        self.chain_success()