#!/bin/bash python
# -*- coding: utf-8 -*-

from pyspark.sql.types import StructType,StringType,IntegerType,DoubleType

sas_type_data_translator = {
    'Char': StringType,
    'Num': IntegerType,
    'Float': DoubleType
}

def translate_types(meta:dict):
    """
    This functions evaluate the meta from sas proc content sheet
    and translate to spark sql type
    meta: {'Format': '$6.00 ', 'column': '1', 'Len': '6',
    'Label': 'aimisdate', 'Informat': '$6.00 ',
    'Variable': 'aimisdate', 'Type': 'Char'}
    """
    sas_type = meta.get('Type',None)
    sas_len = meta.get('Len',None)
    sas_format = meta.get('Format',None)
    sas_informat=meta.get('Informat',None)

    if sas_type == 'Num':
        new_type = get_num_type(sas_format)
    else:
        new_type= sas_type_translator.get(sas_type,None)

    return new_type

def get_num_type(sas_format:str ):
    """
    This funtction return the correct spark type for the
    sas number type according the format
    """
    int_part, dec_part  = sas_format.replace('$','').split('.')

    return 'Float' if int(dec_part) > 0 else 'Num'

