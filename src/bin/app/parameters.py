# !/usr/bin/env python
# -*- coding: utf-8 -*- version: 3.7 -*-
from dataclasses import dataclass, field
from typing import Dict, List, NoReturn
import datetime as dt
import collections
import logging
import glob
import json
import os


@dataclass
class ParmConfiguration:
    """
	This class sets specific arguments for a job/

    Parameters:
        path: ([String]) Complete path from root to the config/ that contains json files.

        job: ([String]) Name of the job and complete pipeline/script in bin/job/.

        attr: ([Dictionary]) The dictionary that will be set as parameters.


    Returns:
        [None]: Sets atributes as a dictionary.

    """
    path: str
    job: str
    attr: Dict = field(init = False)

    def __post_init__(self: object) -> NoReturn:
        self.get_params()

    def get_params(self: object) -> NoReturn:
        """
        If the configuration file has an empty date, today is taken as a reference and we see the last day one month ago.

        If the date is there, it should be a year-month, you have to convert it to datetime taking the first day of the month, and doing the same treatment.

    	"""
        self.file_name = "parameters"
        self.file_comp = self.path + "/" + self.file_name + ".json"
        self.attr = {}

        with open(self.file_comp) as f:
            config = json.load(f)

	    # Primero el año
        json_config = config.get(f"{self.file_name}")

        year = str(json_config.get("process_date"))

        if year == "default":
            today = dt.date.today().replace(day = 1)
        else:
            anio = int(year[:4])
            mes = int(year[-2:]) + 1
            today = dt.date(anio, mes, 1).replace(day = 1)

        real_date = today - dt.timedelta(days = 1)
        real_date = real_date.strftime('%Y%m')
        self.attr["year"] = int(real_date)
        # Lo demás del job
        other_params = json_config.get(f"{self.job}")

        if other_params != None:
            for k, v in other_params.items():
                self.attr[f"{k}"] = v

