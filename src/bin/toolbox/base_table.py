#!/usr/bin/env python
#-*- coding: utf-8 -*-
from dataclasses import dataclass, field
from typing import List, Dict, NoReturn
from os import getenv

@dataclass
class DataType:
    field_name: str
    field_type: str

    def __str__(self: object) -> str:
        return str(self.field_name)

@dataclass
class BaseTable:
    tables: Dict
    table_name: str = field(init = False)
    table_prod_db: str = field(init = False)
    table_prod_path: str = field(init = False)
    table_uat_db: str = field(init = False)
    table_uat_path: str = field(init = False)
    table_sbx_db: str = field(init = False)
    table_sbx_path: str = field(init = False)
    table_layer: str = field(init = False)
    table_partition_field: str = field(init = False)
    env: str = getenv("ENVIRONMENT_NAME")

    def init_base_attr(self: object, name: str) -> NoReturn:
        """
        Set the base attrs for the tables used in the processes
        """
        self.table = self.tables.get(name)
        self.table_name = self.table.get("table_name")
        self.table_prod_db = self.table.get("table_prod_db")
        self.table_prod_path = self.table.get("table_prod_path")
        self.table_uat_db = self.table.get("table_uat_db")
        self.table_uat_path = self.table.get("table_uat_path")
        self.table_sbx_db = self.table.get("table_sbx_db")
        self.table_sbx_path = self.table.get("table_sbx_path")
        self.table_partition_field = self.table.get("table_partition_field")
        self.columns = self.table.get("columns")

    def get_fields(self: object, name: str) -> List:
        """Generate a list with the properties of the object"""
        self.init_base_attr(name)
        if self.columns != None:
            properties = [prop.get("field_hive") for prop in self.columns]
            return properties
        else:
            print("Cannot get fields because it doesn't contained in your json file")


    def get_full_name(self: object, name: str) -> str:
        """
        Given the environment name, it returns the whole table name needed.

        Example:
            Table.get_full_name(env='uat') -> '<database_name>.<table_name>'
        """
        self.init_base_attr(name)

        if self.env.lower() == 'prod':
            return str(self.table_prod_db) + "." + str(self.table_name)

        elif self.env.lower() == 'uat':
            return str(self.table_uat_db) + "." + str(self.table_name)

        elif self.env.lower() == 'sbx':
            return str(self.table_sbx_db) + "." + str(self.table_name)

        else:
            return str(self.table_dev_db) + "." + str(self.table_name)

    def get_db_name(self: object, name: str) -> str:
        """
        Given the environment name, it returns the whole table name needed.

        Example:
            Table.get_db_name(env='uat') -> '<database_name>'
        """
        self.init_base_attr(name)

        if self.env.lower() == 'prod':
            return str(self.table_prod_db)

        elif self.env.lower() == 'uat':
            return str(self.table_uat_db)

        elif self.env.lower() == 'sbx':
            return str(self.table_sbx_db)

        else:
            return str(self.table_dev_db)

    def get_table_name(self: object, name: str) -> str:
        """
        Given the environment name, it returns the whole table name needed.

        Example:
            Table.get_table_name(env='uat') -> '<table_name>'
        """
        self.init_base_attr(name)
        return str(self.table_name)

    def extract_field_list(self: object, name: str) -> List:
        self.init_base_attr(name)


        if self.columns != None:
            field_list = []

            for col in self.columns:
                if not col.get("field_hive") == self.table_partition_field:
                    obj_field = col.get("field_hive")
                    obj_type = col.get("field_type")
                    str_field = str(obj_field) + " " +\
                                str(obj_type.capitalize())
                    field_list.append(str_field)

            return field_list

        else:
            print("Cannot get fields because it doesn't contained in your json file")


    def get_DDL(self: object, name: str) -> str:

        if self.columns != None:
            self.init_base_attr(name)
            field_list = self.extract_field_list(name)
            print("partition: ", self.table_partition_field)

            for col in self.columns:
                print(f"{col}")
                field = col.get("field_hive")
                print(f"{field}")
                if field == self.table_partition_field:
                    print(f"{col}")
                    partition_field = self.table_partition_field + " " +\
                                      col.get("field_type")

            statement = "CREATE TABLE IF NOT EXISTS " +\
                        self.get_full_name(name) +\
                        " (" +  ",\n   ".join(field_list) +\
                        ") PARTITIONED BY (" +\
                        partition_field + ")"
            return statement

        else:
            print("Cannot get fields because it doesn't contained in your json file")


    def build_fields(self: object, name: str) -> NoReturn:
        """
        Read a List of Dict (field,field_hive,field_type) and set
        each element with the attribute of Datatype
        """
        self.init_base_attr(name)

        if self.columns != None:
            for item in self.columns:
                temp_obj = DataType(field_name = item.get("field_hive"),
                                    field_type = item.get("field_type"))
                setattr(self, item["field"], temp_obj)
        else:
            print("Cannot get fields because it doesn't contained in your json file")

