# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-

from dataclasses import dataclass, field
from typing import Dict, List, Union, NoReturn
from collections import OrderedDict
import json
import collections
import logging
import os
import glob

# New DataType
DictNames = Dict[str, str]

@dataclass
class ExtractJson:
    """
    Class used for extract sections in a path of .json files

    Parameters
    ----------

    env: string, name Of each environment

    job: string, name of the job or script

    path: string, the complete name of the path in the server which contains the json files

    parm: dictionary, a dictionary wich contains parameters given by the $CONFIG_PATH/parameters.json

    """
    @classmethod
    def __init__(self, env: str, job: str, path: str, parm: object) -> NoReturn:
        """
        Function that is the constructor in an object-oriented approach.
        The __init__ function is called every time an object is created from a class.
        """
        self.env = env
        self.job = job
        self.path = path
        self.parm = parm

    @classmethod
    def get_list_of_files(self: object) -> List:

        """
            This function returns a list of strings with the complete path and name of the differents files

            Args:
                self.path    ([String]): Path of the json files

            Returns:
                [Pyspark Dataframe]: list of strings which contains each json file
        """
        allFiles = []
        for dirpath, _, filenames in os.walk(self.path):
            for f in filenames:
                allFiles.append(os.path.abspath(os.path.join(self.path, f)))

        allFiles = list(OrderedDict.fromkeys(allFiles))

        return allFiles

    @classmethod
    def load_all_files(self: object) -> DictNames:
        """
            This function returns a dictionary of strings with the complete path and name of the differents files

            Args:
                self.path ([String]): Path of the json files

            Returns:
                [DictNames]: Dict of strings which contains each json file
        """
        list_of_dicts = []

        for file in self.get_list_of_files():
            data = {}

            if file.endswith(".json"):
                print(f"loading: {file}")
                with open(file) as f:
                    data = json.load(f)
                    list_of_dicts.append(data)
                    f.close()

        dict_final = {k: v for x in list_of_dicts \
                            for k, v in x.items()}
        return dict_final

    @classmethod
    def extract_section(self: object, full_json: Dict, key_var: str, env: str, job: str) -> DictNames:
        """
            This function returns a dictionary of strings with the complete path and name of
            the differents files

            Args:
                full_json       ([Dict]): Path of the json files
                key_var         ([Str]): The key of the json file
                env             ([Str]): Name of the environment
                job             ([Str]): Name of the job or a script that contains the pipelines

            Returns:
                [DictNames]: Dict of strings which contains each json file named as the key_var.

        """

        if key_var not in full_json :
            raise Exception(f"""
                The key {key_var} doesn't exist in the json file \
                wich must contain environment variables.""")
        else:

            key_section = full_json.get(f"{key_var}")

            if key_section.get("default") is not None:
                default_section = key_section.get("default")

                if key_section.get("env") is None:
                    dict_section = key_section.get(f"{job}")
            else:
                default_section = {}

            if key_var == "app_config" and dict_section.get("spark_config") == "default":
                spark_conf = key_section.get("default")
                dict_section.update({"spark_config": spark_conf})

            else:
                env_section = key_section.get("env")

                if env_section != None:
                    dict_section = env_section.get(f"{env}")
                else:
                    dict_section = key_section

            if default_section != None:
                # Merge the default settings, the settings that works in both envs
                dict_section = {**dict_section, **default_section}

            dict_job = {f"{key_var}": dict_section}

            return dict_job

    @classmethod
    def get_sections(self: object) -> Dict:

        """
            This function returns a dictionary of strings with the complete path and name of the differents files

            Args:
                self    ([Object]): The object that contains all the needed atributes.

            Returns:
                [Dict]: Dict of Dict, each key is an string that is the principal key of the json file.
        """

        all_sections = {}
        full_json = self.load_all_files()
        keys = list(filter(None, full_json.keys()))

        # Settings of this object
        for k in keys:
            section = self.extract_section(full_json, f"{k}", self.env, self.job)
            all_sections.update(section)

        return all_sections


@dataclass
class AppConfiguration(ExtractJson):
    """
    Class used for extract sections in a path of .json files
    and then sett them as atributes.

    Parameters
    ----------

    env: string, dataframe of the pivot

    job: string, path where we want to save the tables

    path: string, all the constants and information of the sources we want to make the extraction

    parm: dictionary, a dictionary wich contains parameters of the job

    """
    @classmethod
    def __init__(self, env: str, job: str, path: str, parm: object) -> NoReturn:
        """
        Function that is the constructor in an object-oriented approach.
        The __init__ function is called every time an object is created from a class.
        """
        self.env = env
        self.job = job
        self.path = path
        self.parm = parm

        # The attributes that give us information about the class
        all_sections = self.get_sections()

        for k, v in all_sections.items():
            if k != None or v != None:
                setattr(self, k, v)

        for k, v in parm.attr.items():
            setattr(self, k, v)

