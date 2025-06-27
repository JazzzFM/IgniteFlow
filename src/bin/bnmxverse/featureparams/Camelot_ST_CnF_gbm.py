# -*- coding: utf-8 -*-

import pandas as pd
import os

###################################################################################################################
# 1-99 Cap and Floor 
###################################################################################################################

def apply_cap_n_floor(df):
    """
    Description:
        -Apply cap and floor with specific and previously calculated boundaries
    Parameters:
        -df: pandas dataframe to be capped and floored.
        -boundaries: dictionary containing variable name, lower and upper bound
        -features: list of strings containing the names of features to be
                   considered. If not specified, all columns will be considered.     
    Returns:
        -Pandas dataframe with series capped and floored
    """
    boundaries = {
            'income_fix2_sum_tefs_0_capped_scaled': {
                            'lower_bound': 0.006235402612946928, 
                            'upper_bound': 0.7229334968328489
            }, 
            'max_t_act_a2_12m': {
                            'lower_bound': 0.0, 
                            'upper_bound': 216791.0
            }, 
            'rstdsum_pct_pt10_6m': {
                            'lower_bound': 0.0, 
                            'upper_bound': 0.4082482904638629
            }, 
            'dmaxavg_tot_baja_3m': {
                            'lower_bound': 0.0, 
                            'upper_bound': 4290.666666666667
            }, 
            'mns_vp_tot_baja': {
                            'lower_bound': -0.3292972755579242, 
                            'upper_bound': 58.34530155522439
            }, 
            'patron_pago_fix2_tef1_rchif_0m': {
                            'lower_bound': -99999.0, 
                            'upper_bound': 0.6025641025641025
            }, 
            'mob': {
                            'lower_bound': 1.0, 
                            'upper_bound': 385.0
            }, 
            'mns_rt_monto_abonos': {
                            'lower_bound': 0.0, 
                            'upper_bound': 29.35424691868725
            }, 
            'max_tot_alta_6m': {
                            'lower_bound': 0.0, 
                            'upper_bound': 16508.0
            }, 
            'max_t_act_a5_6m': {
                            'lower_bound': 0.0, 
                            'upper_bound': 166988.0
            }, 
            'am_t_act_i4_3m': {
                            'lower_bound': 0.0, 
                            'upper_bound': 78766.63636363638
            }, 
            'ratio_sum_tot_baja_i2_12m_am_tot_act_i2_12m': {
                            'lower_bound': 0.013589505112454731, 
                            'upper_bound': 0.9902273066502012
            }, 
            'dmaxmin_tot_alta_9m': {
                            'lower_bound': 0.0, 
                            'upper_bound': 16508.0
            }, 
            'ratio_sum_t_bajas_a2_9m_am_t_act_a2_9m': {
                            'lower_bound': -1.0, 
                            'upper_bound': 0.6414611158516661
            }, 
            'rso_tot_baja_12m': {
                            'lower_bound': 0.0, 
                            'upper_bound': 5.469499267350781
            }, 
            'min_monto_total_9m': {
                            'lower_bound': -99999.0, 
                            'upper_bound': 157110.28510000027
            }, 
            'so_tot_cli_9m': {
                            'lower_bound': 0.1111111111111111, 
                            'upper_bound': 0.8781299249279124
            }, 
            'ratio_sum_tot_baja_i2_6m_max_tot_act_i2_6m': {
                            'lower_bound': 0.0011118832522585126, 
                            'upper_bound': 0.3257168458781362
            }, 
            'logmean_income_fix2_sum_tefs_6m': {
                            'lower_bound': -99999.0, 
                            'upper_bound': 11.051903928707087
            }, 
            'ratio_sum_t_bajas_ab_6m_t_act_ab_0': {
                            'lower_bound': -1.0, 
                            'upper_bound': 0.9088535485402326
            }, 
            'rmaxavg_tot_baja_12m': {
                            'lower_bound': 1.1484474691620588, 
                            'upper_bound': 12.0
            }, 
            'logmean_tot_act_i4_12m': {
                            'lower_bound': 2.4350742760401247, 
                            'upper_bound': 11.239570270914458
            }, 
            'patron_pago_tef1_rch_0m_5c': {
                        'lower_bound': 0.0, 
                        'upper_bound': 8.0
            }, 
            'rminavg_t_bajas_a4_3m': {
                        'lower_bound': 0.0, 
                        'upper_bound': 0.9688715953307392
            }
    }
    features = ['income_fix2_sum_tefs_0_capped_scaled', 'max_t_act_a2_12m', 'rstdsum_pct_pt10_6m', 'dmaxavg_tot_baja_3m', 'mns_vp_tot_baja', 'patron_pago_fix2_tef1_rchif_0m', 'mob', 'mns_rt_monto_abonos', 
              'max_tot_alta_6m', 'max_t_act_a5_6m', 'am_t_act_i4_3m', 'ratio_sum_tot_baja_i2_12m_am_tot_act_i2_12m', 'dmaxmin_tot_alta_9m', 'ratio_sum_t_bajas_a2_9m_am_t_act_a2_9m', 'rso_tot_baja_12m', 
              'min_monto_total_9m', 'so_tot_cli_9m', 'ratio_sum_tot_baja_i2_6m_max_tot_act_i2_6m', 'logmean_income_fix2_sum_tefs_6m', 'ratio_sum_t_bajas_ab_6m_t_act_ab_0', 'rmaxavg_tot_baja_12m', 
              'logmean_tot_act_i4_12m', 'patron_pago_tef1_rch_0m_5c', 'rminavg_t_bajas_a4_3m']
    if features == None:
        features = df.columns.tolist()
    else:
        features = features
    aux_df = df.copy()
    for item in features:
        lb = boundaries[item]['lower_bound']
        ub = boundaries[item]['upper_bound']
        aux_df[item].clip(lb, ub, inplace=True)
    return aux_df
