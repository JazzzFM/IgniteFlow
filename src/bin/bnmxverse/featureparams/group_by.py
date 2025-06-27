"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import os
import pandas as pd
from pyspark.sql.functions import greatest, expr, col, broadcast, month, when, expr, lit, create_map, lower, array, explode, struct, concat_ws
from pyspark.sql.types import IntegerType, DoubleType
from itertools import chain


class BureauPullAggregate:
    """
    Class to aggregate Bureau Pull Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _create_groupby_query(self, query_dict, group_by):
        """
        Creates the SQL aggregation sentence to group the bureau pull data
        
        Parameters
        ----------
        query_dict: Dictionary
            All parameters needed to create SQL sentence
        group_by: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(data=query_dict)
        agg_df = agg_df[agg_df["variable"] != ""]
        features = (','.join(['{0}(case when {1}> 0 then {1} else 0 end ) as {2}'.
                    format(item['agg'], item['variable'], item['feature'])
                    for _, item in agg_df.iterrows()]))
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+features + \
            " FROM tempBureau GROUP BY "+group_str
        return query_str

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)

    def getCostumerBureau(self, df, aggs, sqlContext):
        """
        Calculate new bureau variables.

        Parameters
        ----------
        df: Spark Dataframe
            Data to work with
        aggs: Dictionary
            All parameters needed by the class(key, group_by, querys)
        sqlContext: Spark context
            Context working with

        Returns
        -------
        Spark Dataframe
            Dataframe with new calculated columns
        """
        # print(aggs.feature)
        group_by = ["numcliente", "vintage_date"]
        tfrom = "tfrom"
        timeWindow = 6
        groupBy = ",".join(group_by)
        # Get Months on Bureau and Hit bureau
        df.createOrReplaceTempView("dfTempFlag")
        mob = "max(a5+{0}) over (partition by {1}) as mob".format(tfrom, groupBy)
        hit_adv = "case when sum((case when a5 is null then 0 else 1 end)) over (partition by {0})>0 then 1 else 0 end as hit_adv ".format(
            groupBy)
        nmiss = "sum((case when a5 is null then 1 else 0 end)) over (partition by {0}) as nmiss".format(
            groupBy)
        flags = ",".join([mob, hit_adv, nmiss])
        query = "select *,{0} from dfTempFlag order by {1} ".format(
            flags, groupBy)
        dfBureau = sqlContext.sql(query)
        # Get flags missings
        dfBureau.createOrReplaceTempView("dfTempMiss")
        fmiss = "case when (hit_adv=1) and (a5 is null) and mob>tfrom then 1 else 0 end as fmiss"
        flagNews = "case when (hit_adv=1) and mob<=12 then 1 else 0 end as flag_new"
        cumulative_miss = "sum((case when a5 is null then 1 else 0 end)) over (partition by {0} order by tfrom rows between 12 preceding and current row ) as cumulative".format(
            groupBy)
        flag = ",".join([fmiss, flagNews, cumulative_miss])
        query = "select *,{0} from dfTempMiss order by {1}".format(
            flag, groupBy)
        dfBureauMiss = sqlContext.sql(query)
        # get flag missing
        dfBureauMiss.createOrReplaceTempView("dfFlags")
        flag_6m = "case when max(cumulative*(case when tfrom={0} then 1 else 0 end)) over (partition by {1})={2} then 1 else 0 end as flagMissing".format(
            timeWindow-1, groupBy, timeWindow)
        flag_7m = "case when max(cumulative) over (partition by {0}) >=7 then 1 else 0 end as flag_cum".format(
            groupBy)
        cumulativef = ",".join(["case when sum(case when cumulative<={0} then 1 else 0 end) over (partition by {1})>0 then 1 else 0 end as f{0}".format(
            item, groupBy) for item in range(12)])
        fillNew = "case when (hit_adv=0) or ((flag_new=1) and (fmiss=0) and (a5 is null)) then 1 else 0 end as fill0"
        flag = ",".join([flag_6m, flag_7m, fillNew])
        query = "select *,{0} from dfFlags order by {1}".format(flag, groupBy)
        dfBureauMiss2 = sqlContext.sql(query)
        # Fill sistem
        dfBureauMiss2.createOrReplaceTempView("dfFill")
        hitBureau = "case when (hit_adv=1) and (flagMissing=0) and (flag_cum=0) and (flag_new=0) then 1 else 0 end as hitBureau"
        fill = ",".join(["case when fill0=1 then 0 else {0} end as {0}".format(
            item) for item in aggs["feature"] if item != ""])
        flag = ",".join([groupBy, tfrom, hitBureau, fill])
        query = "select {0} from dfFill order by {1}".format(flag, groupBy)
        dfBureauMiss3 = sqlContext.sql(query)
        return dfBureauMiss3

    def aggregate(self):
        """
        Process to aggregate bureau pull data.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        reading_path = os.path.join(
            self.previous_path, self.agg_dict['table_name'])
        df_in = self.sqlContext.read.parquet(reading_path)
        df_in.createOrReplaceTempView("tempBureau")
        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'])
        df_out = self.sqlContext.sql(query_str)
        df_gcb = self.getCostumerBureau(
            df_out, self.agg_dict['query_dict'], self.sqlContext)
        head, _ = os.path.split(self.previous_path)
        self._save_parquet(df_gcb, os.path.join(
            head, "groupby", self.agg_dict['table_name']))


class RevBalanceAggregate:
    """
    Class to aggregate Revolving Balance Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)

    def epp_aggregate(self, sqlContext, epp_path, agg_dict, epp_dict):
        """
        Agregates EPP Data.

        Parameters
        ----------
        sqlContext: Spark context
            Context working in
        epp_path: string
            Path to read EPP Data From
        aggs: Dictionary
            All parameters needed in the aggregation process

        Returns
        -------
        Spark Dataframe
            Dataframe with aggregated data.
        """
        import pyspark.sql.functions as fn
        promoID = sqlContext.sql(
            "select  *  from {0}".format(agg_dict['table_names']['catPromosID']))
        promoID = promoID.withColumnRenamed("promo id", "promoId").select(
            ["promoId", "grupo"]).withColumnRenamed("grupo", "oferta")

        epp_df = sqlContext.read.parquet(
            epp_path).withColumnRenamed("proc_dt", "mis_dt")
        epp_df = epp_df.filter("estatus = 1")
        epp_df = epp_df.join(broadcast(promoID), on=[
                             epp_df.promo_id == promoID.promoId], how="left_outer")
        epp_df = epp_df.withColumn('plazo', epp_df['plazo'].cast(DoubleType()))
        epp_df = epp_df.withColumn(
            'month', month(epp_df['mis_dt']).alias('month'))

        epp_df = epp_df.withColumn('oferta',
                                   when((epp_df['oferta'].isNull()) & (epp_df["promo_id"][0:1] == '7'), 'T0').
                                   when((epp_df['oferta'].isNull()) & (epp_df["promo_id"][0:2] == '03'), 'T0').
                                   when((epp_df['oferta'].isNull()) & (epp_df["promo_id"][0:2] == '1T'), 'T0').
                                   when((epp_df['oferta'].isNull()) & (epp_df["promo_id"][0:5] == '4DBAL'), 'Balcon').
                                   when((epp_df['oferta'].isNull()) & (epp_df["promo_id"][0:3] == '4DB'), 'Disponible').
                                   when((epp_df['oferta'].isNull()) & (epp_df["promo_id"][0:2] == '5E'), 'Efectivo').
                                   when((epp_df['oferta'].isNull()) & (epp_df["promo_id"][0:2] == '2P'), 'PPP').
                                   when((epp_df['oferta'].isNull()) & (epp_df["promo_id"][0:2] == '6R'), 'RES').
                                   otherwise(epp_df['oferta']))

        tipo_ofertas = ['Balcon', 'Disponible', 'Efectivo', 'PPP', 'T0', 'RES']
        sel_cols = ['original_balance', 'current_balance',
                    'tasa', 'plazo', 'plazo_actual']
        list_new_cols = []

        for oferta in tipo_ofertas:
            for col in sel_cols:
                new_col_name = col + '_' + oferta
                list_new_cols.append(new_col_name)
                epp_df = epp_df.withColumn(new_col_name, when(
                    epp_df['oferta'] == oferta, epp_df[col]).otherwise(None))

        # filter aggs
        agg_cols_sum = [c for c in list_new_cols if (
            'original_balance' in c) | ('current_balance' in c)]
        agg_cols_avg = [c for c in list_new_cols if (
            'tasa' in c) | ('plazo' in c) | ('plazo_actual' in c)]
        # Create aggs
        expr_sum = [fn.sum(epp_df[c]).alias("EPP_"+c) for c in agg_cols_sum]
        expr_avg = [fn.avg(epp_df[c]).alias("EPP_"+c) for c in agg_cols_avg]
        others = [fn.max('Fecha_Aceptacion').alias("EPP_MAX_of_Fecha_Aceptacion"), fn.min('Fecha_Aceptacion').alias(
            "EPP_MIN_of_Fecha_Aceptacion"), fn.avg('Tasa').alias("EPP_AVG_of_Tasa_Total"), fn.avg('Plazo').alias("EPP_AVG_of_Plazo_Total")]
        expr_agg = expr_sum + expr_avg + others
        # groupBy=['CRD_ACCT_NBR','MIS_DT_EPP','month']
        epp_agg = epp_df.groupBy(
            ["crd_acct_nbr", "vintage_date", "tfrom"]).agg(*expr_agg)

        return epp_agg

    def plan_aggregate(self, sqlContext, plan_path):
        """
        Agregates EPP Data.

        Parameters
        ----------
        sqlContext: Spark context
            Context working in
        plan_path: string
            Path to read Plan Data From
        aggs: Dictionary
            All parameters needed in the aggregation process

        Returns
        -------
        Spark Dataframe
            Dataframe with aggregated data.
        """
        import pyspark.sql.functions as fn
        plan_df = sqlContext.read.parquet(plan_path)
        plan_df = plan_df.withColumn(
            'month', month(plan_df['mis_dt']).alias('month'))

        product = {'Compras': col('plan_nbr')[0:2] == '10',
                   'TMAX': col('plan_nbr')[0:2].isin(['11', '21']),
                   'Cash': col('plan_nbr')[0:2] == '20',
                   'T0': col('plan_nbr') == '90400',
                   'Efec': col('plan_nbr').isin(['91001', '91000']),
                   'DB': col('plan_nbr').isin(['90000', '90001']),
                   'PPP': col('plan_nbr') == '90100',
                   'Rewrites': col('plan_nbr') == '91200'
                   }
        var = ['current_balance', 'int_grav_exent', 'tasa']
        for itemProduct in product:
            for itemVar in var:
                plan_df = plan_df.withColumn(
                    itemVar+'_'+itemProduct, when(product[itemProduct], col(itemVar)).otherwise(lit(0)))

        agg_cols_sum = [item+'_'+itemProduct for item in var[0:2]
                        for itemProduct in product]
        agg_cols_avg = [var[2]+'_'+itemProduct for itemProduct in product]
        # Create aggs
        expr_sum = [fn.sum(col(c)).alias("Plan_"+c) for c in agg_cols_sum]
        expr_avg = [fn.avg(col(c)).alias("Plan_"+c) for c in agg_cols_avg]
        others = [fn.max('tasa_compras').alias("plan_max_tasa_compras"), fn.max(
            'tasa_cash').alias("plan_max_tasa_cash")]
        expr = expr_sum + expr_avg + others

        groupBy = ["crd_acct_nbr", "vintage_date", "tfrom"]
        dfAgg = plan_df.groupBy(groupBy).agg(*expr)
        newVars = {
            "Total_Balance": "current_balance",
            "Total_Intereses": "int_grav_exent"
        }
        total = ["({1}) as {0}".format(itemNewVar, "+".join(["Plan_{0}_{1}".format(
            newVars[itemNewVar], itemProduct) for itemProduct in product])) for itemNewVar in newVars]
        query = "select *,{0} from dfPlanesTemp".format(",".join(total))
        dfAgg.createOrReplaceTempView("dfPlanesTemp")
        plan_agg = sqlContext.sql(query)

        return plan_agg

    def revbalance_aggregate(self, ccp_path, agg_dict, epp_agg, plan_agg):
        """
        Joins together the epp and plan data. Credit Card level is aggregated
        
        Parameters
        ----------
        ccp_path: string
            Path to read CCP Data From
        agg_dict: Dictionary
            All parameters needed by the class(key, group_by, querys)
        epp_agg: Spark Dataframe
            EPP Dataframe
        plan_agg: Spark Dataframe
            Plan Dataframe

        Returns
        -------
        Spark Dataframe
            Dataframe with Joined Data
        """
        from pyspark.sql.functions import col, sum, when, count, round
        cpp_df = self.sqlContext.read.parquet(ccp_path)
        ccpAgg = cpp_df.select(
            agg_dict['primaryKey']+["eir", "monthsonbooks", "numcliente"])

        pivotHist1 = ccpAgg.join(epp_agg, agg_dict['primaryKey'], how="left")
        pivotHist1 = pivotHist1.join(
            plan_agg, agg_dict['primaryKey'], how="left")

        pivotHist1.createOrReplaceTempView("tempHis")
        groupVariables = agg_dict['group_by_key']
        dropVar = ["crd_acct_nbr"]
        dropVar.extend(groupVariables)
        listFeatures = [
            item for item in pivotHist1.columns if item not in dropVar]
        listFeaturesSum = [item for item in listFeatures if "tasa" not in item.lower(
        ) and "fecha" not in item.lower()]
        listFeaturesMax = [
            item for item in listFeatures if "tasa" in item.lower()]
        querySum = ["sum({0}) as {0}".format(item) for item in listFeaturesSum]
        querymax = ["max({0}) as {0}".format(item) for item in listFeaturesMax]
        #queryAux=["sum(case when {0} is null then 0 else {0} end) as _{0}".format(item) for item in ["Total_Intereses","Plan_int_grav_exent_Compras","eir"]]
        query = ",".join(querySum+querymax)
        groupBy = ",".join(groupVariables)
        queryAgg = "select {0},{1} from tempHis GROUP BY {0}".format(
            groupBy, query)

        histCliente = self.sqlContext.sql(queryAgg)
        histCliente1 = histCliente.withColumn("total_interes_fin", when(round("Total_Intereses", 6) == round(
            "eir", 6), round("eir", 6)).otherwise(col("Total_Intereses")+col("Plan_int_grav_exent_Compras")-round("eir", 6)))
        histCliente1 = histCliente1.withColumn("total_bal_rev", when(
            col("total_interes_fin") > 0, col("Total_Balance")).otherwise(0))
        histCliente2 = histCliente1.withColumn("rev_flag", when(
            col("total_interes_fin") > 0, 1).otherwise(0))
        histCliente2.createOrReplaceTempView("dfCast")
        cast = ",".join(["cast({0} as double)".format(item)
                        for item in histCliente2.columns if item not in dropVar])
        query = "select {0},{1} from dfCast".format(groupBy, cast)

        rev_plan_agg = self.sqlContext.sql(query)

        return rev_plan_agg

    def aggregate(self):
        """
        Process to aggregate Revolving Balance data.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        reading_path_epp = os.path.join(
            self.previous_path, self.agg_dict['table_names']["epp"])
        epp_dict = self.agg_dict['querys_dict']["epp_dict"]
        epp_agg = self.epp_aggregate(
            self.sqlContext, reading_path_epp, self.agg_dict, epp_dict)

        reading_path_plan = os.path.join(
            self.previous_path, self.agg_dict['table_names']["plan"])
        plan_agg = self.plan_aggregate(self.sqlContext, reading_path_plan)

        reading_path_cpp = os.path.join(
            self.previous_path, self.agg_dict['table_names']["cpp"])

        revbal_agg = self.revbalance_aggregate(
            reading_path_cpp, self.agg_dict, epp_agg,  plan_agg)

        # revbal_agg.withColumn("flag_rev",when(col("numcliente").isNotNull(),1).otherwise(0))
        head, _ = os.path.split(self.previous_path)
        #work_path, prev_step = os.path.split(head)
        self._save_parquet(revbal_agg, os.path.join(head, "groupby"))
        print("Finish aggregating")


class LiabilitiesPullAggregate:
    """
    Class to aggregate Liabilities Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _create_groupby_query(self, agg_dict, liab):
        """
        Creates the SQL aggregation sentence to group the liabilities data
        
        Parameters
        ----------
        agg_dict: Dictionary
            All parameters needed by the class(key, group_by, querys)
        liab: string
            Liabilities Product Source

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(data=agg_dict["query_dict"])
        agg_df = agg_df[agg_df["variable"] != ""]
        features = (','.join([' {0}({1}) as {1}'.
                    format(item['agg'], item['variable'])
                    for _, item in agg_df.iterrows()]))
        group_str = ",".join(agg_dict["group_by_key"])
        query_str = "SELECT "+group_str+","+features + \
            " FROM df_agg_temp_df_" + liab + " GROUP BY "+group_str
        return query_str

    def _create_groupby_query_pagomatico(self, agg_dict):
        """
        Creates the SQL aggregation sentence to group the Pagomatico data
        
        Parameters
        ----------
        agg_dict: Dictionary
            All parameters needed by the class(key, group_by, querys)

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(data=agg_dict["PGMT_QUERY_DICT"])
        agg_df = agg_df[agg_df["variable"] != ""]
        features = (','.join([' {0}({1}) as {1}'.
                    format(item['agg'], item['variable'])
                    for _, item in agg_df.iterrows()]))
        group_str = ",".join(agg_dict["group_by_key"])
        query_str = "SELECT "+group_str+","+features + \
            " FROM df_agg_temp_pgom GROUP BY "+group_str
        return query_str

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)

    def aggregate(self):
        """
        Process to aggregate bureau pull data.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        head, _ = os.path.split(self.previous_path)
        aux_path = os.path.join(head, "auxiliar/outputs")

        for key, value in self.agg_dict['table_names'].items():
            liabilities_df = self.sqlContext.read.parquet(
                os.path.join(self.previous_path, value))
            #liabilities_df = liabilities_df.withColumnRenamed("num_cliente","numcliente")
            liabilities_df = liabilities_df.filter(
                liabilities_df.numcliente.isNotNull())
            liabilities_df = liabilities_df.select(
                self.agg_dict["group_by_key"] + self.agg_dict["query_dict"]["variable"])
            liabilities_df.createOrReplaceTempView("df_agg_temp_df_"+key)
            sql_query = self._create_groupby_query(self.agg_dict, key)
            liabilities_df = self.sqlContext.sql(sql_query)
            liabilities_df = liabilities_df.withColumn('src', lit(key))
            liabilities_df = liabilities_df.fillna(0)
            name = "df_"+key
            save = os.path.join(aux_path, name)
            self._save_parquet(liabilities_df, save)

        pagomatico_df = self.sqlContext.read.parquet(
            os.path.join(self.previous_path, self.agg_dict["pagomatico"]))
        pagomatico_df = pagomatico_df.withColumnRenamed(
            "num_cliente", "numcliente")
        pagomatico_df = pagomatico_df.filter(
            pagomatico_df.numcliente.isNotNull())
        pagomatico_df = pagomatico_df.select(
            self.agg_dict["group_by_key"] + self.agg_dict["PGMT_QUERY_DICT"]["variable"])
        pagomatico_df.createOrReplaceTempView("df_agg_temp_pgom")
        sql_query = self._create_groupby_query_pagomatico(self.agg_dict)
        pagomatico_df = self.sqlContext.sql(sql_query)
        pagomatico_df = pagomatico_df.withColumn('src', lit("pgom"))
        pagomatico_df = pagomatico_df.withColumnRenamed(
            "sdo_capital", "saldo_mensual")
        pagomatico_df = pagomatico_df.fillna(0)
        self._save_parquet(pagomatico_df, os.path.join(aux_path, "df_pgom"))

        df_maes = self.sqlContext.read.parquet(aux_path+"/df_maes")
        df_perf = self.sqlContext.read.parquet(aux_path+"/df_perf")
        df_peje = self.sqlContext.read.parquet(aux_path+"/df_peje")
        df_cheq = self.sqlContext.read.parquet(aux_path+"/df_cheq")
        df_otro = self.sqlContext.read.parquet(aux_path+"/df_otro")
        df_pgom = self.sqlContext.read.parquet(aux_path+"/df_pgom")

        columns_to_add = [
            x for x in df_perf.columns if x not in df_pgom.columns]
        for column in columns_to_add:
            df_pgom = df_pgom.withColumn(column, lit(None))

        all_liabilities = df_maes.unionByName(df_perf)\
            .unionByName(df_peje)\
            .unionByName(df_cheq)\
            .unionByName(df_otro)\
            .unionByName(df_pgom)

        self._save_parquet(all_liabilities, os.path.join(
            self.previous_path, "appends"))
        all_liabilities = self.sqlContext.read.parquet(
            os.path.join(self.previous_path, "appends"))

        onus_capt_snap_tmp = all_liabilities.groupBy(
            self.agg_dict["group_by_key"]).sum(*self.agg_dict["query_dict"]["variable"])

        for variable in self.agg_dict["query_dict"]["variable"]:
            onus_capt_snap_tmp = onus_capt_snap_tmp.withColumnRenamed(
                "sum("+variable+")", variable)

        liabilities_dfs = [df_maes, df_perf,
                           df_peje, df_cheq, df_otro, df_pgom]

        map_maes = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["maes_new_cols"]))
        map_perf = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["perf_new_cols"]))
        map_peje = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["peje_new_cols"]))
        map_cheq = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["cheq_new_cols"]))
        map_otro = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["otro_new_cols"]))
        map_pgom = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["pgom_new_cols"]))

        rename_maps = [map_maes, map_perf,
                       map_peje, map_cheq, map_otro, map_pgom]

        for idx, src in enumerate(liabilities_dfs):
            onus_capt_snap_tmp = onus_capt_snap_tmp.join(
                src.select([col(column) for column in self.agg_dict["group_by_key"]] +
                           [col(c).alias(rename_maps[idx][c]) for c in self.agg_dict["sel_cols"]]),
                on=self.agg_dict["group_by_key"],
                how='left'
            )

        onus_capt_snap_tmp = onus_capt_snap_tmp.fillna(0)
        head, _ = os.path.split(self.previous_path)
        self._save_parquet(onus_capt_snap_tmp, os.path.join(head, "groupby"))


class ChannelsAggregate:
    """
    Class to aggregate Channels Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)

    def aggregate(self):
        """
        Process to aggregate bureau pull data.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        reading_path = os.path.join(
            self.previous_path, self.agg_dict['table_name'])
        channels_df = self.sqlContext.read.parquet(reading_path)
        channels_df.createOrReplaceTempView("channels")
        total = self.sqlContext.sql(self.agg_dict["query_str"])
        total = total.na.fill(0)

        aggregate_dict = self.agg_dict['query_dict']
        for op in aggregate_dict:
            for new_column in aggregate_dict[op]:
                if op == "flags":
                    total = total.withColumn(new_column, greatest(
                        *aggregate_dict[op][new_column]))
                if op == "sums":
                    total = total.withColumn(new_column, expr(
                        "+".join(aggregate_dict[op][new_column])))

        head, _ = os.path.split(self.previous_path)
        #work_path, prev_step = os.path.split(head)
        self._save_parquet(total, os.path.join(head, "groupby"))


class FinancieraAggregate:
    """
    Class to aggregate Financiera Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _create_groupby_query(self, query_dict, group_by):
        """
        Creates the SQL aggregation sentence to group the Financiera data
        
        Parameters
        ----------
        query_dict: Dictionary
            All parameters needed to create SQL sentence
        group_by: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(query_dict)
        list_aux = []

        for _, item in agg_df.iterrows():
            for sth in item['variable']:
                if isinstance(sth, str):
                    str_aux = item['agg']+"(case when bl in ('"+sth + \
                        "') then saldo else 0 end ) as "+item['feature']
                else:
                    str_aux = item['agg']+"(case when bl in ("+",".join(
                        '\''+iii+'\'' for iii in sth)+") then saldo else 0 end ) as "+item['feature']
                list_aux.append(str_aux)
        query_string = ",".join(list_aux)
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+query_string + \
            " FROM financiera GROUP BY "+group_str
        return query_str

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)

    def aggregate(self):
        """
        Process to aggregate Financiera data.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        cves_dict = {
            2030: 'adela', 2130: 'adela', 1011: 'cheques', 1010: 'cheques', 5000: 'bancanet',
            2100: 'comercial', 2170: 'comercial', 9002: 'credichq', 1020: 'ctamtra', 1040: 'debito',
            2050: 'hipotecario', 2060: 'hipotecario', 2070: 'hipotecario', 2140: 'hipotecario', 2150: 'hipotecario',
            1060: 'inversion', 1090: 'inversion', 1091: 'inversion', 1100: 'inversion', 1110: 'inversion', 1130: 'inversion',
            1150: 'inversion', 1160: 'inversion', 4000: 'inversion', 9001: 'inversion', 1070: 'miahorro',
            9004: 'otros', 9005: 'otros', 9006: 'otros', 9007: 'otros', 9008: 'otros', 9009: 'otros', 9010: 'otros',
            9011: 'otros', 9012: 'otros', 9013: 'otros', 1050: 'pagomatico', 1030: 'perfiles', 2020: 'personales',
            2040: 'personales', 2080: 'personales', 2090: 'personales', 2120: 'personales', 2160: 'personales',
            1080: 'plazo', 1120: 'plazo', 2105: 'pyme', 2173: 'pyme', 3001: 'seguros', 3002: 'seguros', 3003: 'seguros',
            3004: 'seguros', 1140: 'servicios', 1141: 'servicios', 9003: 'servicios',
            1055: 'tarjetas', 2010: 'tarjetas', 2011: 'tarjetas', 2075: 'tarjetas', 2110: 'tarjetas', 2175: 'tarjetas',
            8000: 'na', 1: 'na', 2: 'na', 0: 'na'
        }

        reading_path = os.path.join(
            self.previous_path, self.agg_dict['table_name'])
        financiera = self.sqlContext.read.parquet(reading_path)
        #cves_dict = self.agg_dict['cve_dict']
        mapping_expr = create_map([lit(x) for x in chain(*cves_dict.items())])
        financiera = financiera.withColumn(
            "bl", mapping_expr.getItem(col("cve_ayp")))
        financiera_aux = financiera.select(
            "numcliente", "saldo", "saldoaj", "vintage_date", "tfrom", "bl")
        financiera_aux = financiera_aux.withColumn('bl', lower(col('bl')))
        financiera_aux.createOrReplaceTempView("financiera")
        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'])
        df_out = self.sqlContext.sql(query_str)
        head, _ = os.path.split(self.previous_path)
        #work_path, prev_step = os.path.split(head)
        self._save_parquet(df_out, os.path.join(head, "groupby"))

class MacrogirosAggregate:
    """
    Class to aggregate Macrogiros Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _create_groupby_query(self, query_dict, group_by):
        """
        Creates the SQL aggregation sentence to group the macrogiros data.

        Parameters
        ----------
        query_dict: Dictionary
            Macrogiro Concept- Macrogiros Abbreviation pair
        group_by: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(query_dict.items(), columns=[
            'giros', 'giros_colname'])

        features = (','.join(["sum(case when purchase > 0 and c2 = '{0}' then purchase else 0 end ) as {1},sum(case when purchase > 0 and c2 = '{0}' then purchasetx else 0 end ) as {1}_cnt ".
                    format(item['giros'], item['giros_colname'])
                    for _, item in agg_df.iterrows()]))
        features = features + \
            ",sum(case when purchase > 0 then purchase else 0 end ) as tot_pur, sum(case when purchase > 0 then purchasetx else 0 end ) as no_trans"
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+features + \
            " FROM macrogiros GROUP BY "+group_str
        return query_str

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)
        print("Saving on: ", path)

    def aggregate(self):
        """
        Process to aggregate Macrogiros data.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        reading_path = os.path.join(
            self.previous_path, self.agg_dict['table_name'])
        macrogiros = self.sqlContext.read.parquet(reading_path)
        macrogiros = macrogiros.na.fill("sin clasificar", subset=["c2"])
        macrogiros.createOrReplaceTempView("macrogiros")

        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'])
        df_out = self.sqlContext.sql(query_str)
        head, _ = os.path.split(self.previous_path)
        self._save_parquet(df_out, os.path.join(head, "groupby"))


class ProductDataAggregate:
    """
    Class to aggregate Product Data Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _create_groupby_query(self, query_dict, group_by, tempview):
        """
        Creates the SQL aggregation query string use to group the bureau pull data
        
        Parameters
        ----------
        query_dict: Dictionary
            Variables to consider product tenure
        group_by: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(query_dict)
        agg_df = agg_df[agg_df["variable"] != ""]
        features = (','.join(['{0}(case when {1}> 0 then 1 else 0 end ) as {2}'.
                              format(item['agg'], item['variable'],
                                     item['feature'])
                              for _, item in agg_df.iterrows()]))
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+features + \
            " FROM "+tempview+" GROUP BY "+group_str
        return query_str

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)

    def aggregate(self):
        """
        Process to aggregate bureau pull data.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        reading_path_baseline = os.path.join(
            self.previous_path, self.agg_dict['table_names']["baseline"])
        baseline_df = self.sqlContext.read.parquet(reading_path_baseline)
        reading_path_ccp = os.path.join(
            self.previous_path, self.agg_dict['table_names']["ccp"])
        ccp_df = self.sqlContext.read.parquet(reading_path_ccp)

        query_baseline = self._create_groupby_query(
            self.agg_dict['querys_dict']["baseline_dict"], self.agg_dict['group_by_key'], "baseline")
        query_ccp = self._create_groupby_query(
            self.agg_dict['querys_dict']["ccp_dict"], self.agg_dict['group_by_key'], "ccp")

        baseline_df.createOrReplaceTempView("baseline")
        ccp_df.createOrReplaceTempView("ccp")

        flags_base = self.sqlContext.sql(query_baseline)
        flags_xec_perf = self.sqlContext.sql(query_ccp)

        df_product = flags_base.join(
            flags_xec_perf, on=self.agg_dict['group_by_key'], how="full")

        df_product = df_product.withColumn(
            "tfrom", df_product["tfrom"].cast(IntegerType()))
        df_product = df_product.na.fill(0)

        head, _ = os.path.split(self.previous_path)
        #work_path, prev_step = os.path.split(head)
        self._save_parquet(df_product, os.path.join(head, "groupby"))

        pivot_product_data = GeneratePivot(None, None, self.sqlContext)
        pivot_product_data.pivot_data_aux(os.path.join(head, "groupby"), [
                                          "numcliente", "vintage_date", "tfrom"])

        product_data = self.sqlContext.read.parquet(
            os.path.join(head, "pivot"))
        product_data.createOrReplaceTempView("data_flags1")

        features = ["ind_cnb", "ind_cpb", "ind_mor", "ind_disp", "ind_effec"]
        meses = [3, 6, 9, 12]

        lista_aux = [[feature+"_"+str(idx) for idx in range(12) if feature+"_"+str(
            idx) in product_data.columns] for feature in features]
        lista_aux2 = [prod[:mes] for prod in lista_aux for mes in meses]

        query_Str = "select numcliente, vintage_date,"+",".join("greatest("+",".join(ll)+") as "+ll[0][:-1] + str(
            len(ll)) + "m " for _, ll in enumerate(lista_aux2)) + " from data_flags1"

        df = self.sqlContext.sql(query_Str)
        df.createOrReplaceTempView("data_flags2")

        totl_ = [prod+"_"+str(mes)+"m" for mes in meses for prod in features if prod +
                 "_"+str(mes)+"m" in df.columns]
        totl_1 = [totl_[len(features)*i:len(features)*(i+1)]
                  for i in range(len(meses))]

        testStr2 = "select * ,"+",".join("("+"+".join(ind) + " ) as "+"total_prod_"+str(
            meses[_]) + "m " for _, ind in enumerate(totl_1))+" from data_flags2"

        df2 = self.sqlContext.sql(testStr2)
        self._save_parquet(df2, os.path.join(head, "features"))
        print("Finish featuring")


class BureauCohortAggregate:
    """
    Class to aggregate Bureau Cohort Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _create_groupby_query(self, query_dict, group_by):
        """
        Creates the SQL aggregation sentence to group the Bureau Cohort data
        
        Parameters
        ----------
        query_dict: Dictionary
            All parameters needed to create SQL sentence
        group_by: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(data=query_dict)
        agg_df = agg_df[agg_df["variable"] != ""]
        features = (','.join(['{0}(case when {1}> 0 then {1} else 0 end ) as {2}'.
                    format(item['agg'], item['variable'], item['feature'])
                    for _, item in agg_df.iterrows()]))
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+features + \
            " FROM tempBureau GROUP BY "+group_str
        return query_str

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)

    def getCostumerBureau(self, df, aggs, sqlContext):
        """
        Calculate new bureau variables.

        Parameters
        ----------
        df: Spark Dataframe
            Data to work with
        aggs: Dictionary
            All parameters needed by the class(key, group_by, querys)
        sqlContext: Spark context
            Context working with

        Returns
        -------
        Spark Dataframe
            Dataframe with new calculated columns
        """
        group_by = ["numcliente", "vintage_date"]
        tfrom = "tfrom"
        timeWindow = 6
        groupBy = ",".join(group_by)
        # Get Months on Bureau and Hit bureau
        df.createOrReplaceTempView("dfTempFlag")
        mob = "max(a5+{0}) over (partition by {1}) as mob".format(tfrom, groupBy)
        hit_adv = "case when sum((case when a5 is null then 0 else 1 end)) over (partition by {0})>0 then 1 else 0 end as hit_adv ".format(
            groupBy)
        nmiss = "sum((case when a5 is null then 1 else 0 end)) over (partition by {0}) as nmiss".format(
            groupBy)
        flags = ",".join([mob, hit_adv, nmiss])
        query = "select *,{0} from dfTempFlag order by {1} ".format(
            flags, groupBy)
        dfBureau = sqlContext.sql(query)
        # Get flags missings
        dfBureau.createOrReplaceTempView("dfTempMiss")
        fmiss = "case when (hit_adv=1) and (a5 is null) and mob>tfrom then 1 else 0 end as fmiss"
        flagNews = "case when (hit_adv=1) and mob<=12 then 1 else 0 end as flag_new"
        cumulative_miss = "sum((case when a5 is null then 1 else 0 end)) over (partition by {0} order by tfrom rows between 12 preceding and current row ) as cumulative".format(
            groupBy)
        flag = ",".join([fmiss, flagNews, cumulative_miss])
        query = "select *,{0} from dfTempMiss order by {1}".format(
            flag, groupBy)
        dfBureauMiss = sqlContext.sql(query)
        # get flag missing
        dfBureauMiss.createOrReplaceTempView("dfFlags")
        flag_6m = "case when max(cumulative*(case when tfrom={0} then 1 else 0 end)) over (partition by {1})={2} then 1 else 0 end as flagMissing".format(
            timeWindow-1, groupBy, timeWindow)
        flag_7m = "case when max(cumulative) over (partition by {0}) >=7 then 1 else 0 end as flag_cum".format(
            groupBy)
        cumulativef = ",".join(["case when sum(case when cumulative<={0} then 1 else 0 end) over (partition by {1})>0 then 1 else 0 end as f{0}".format(
            item, groupBy) for item in range(12)])
        fillNew = "case when (hit_adv=0) or ((flag_new=1) and (fmiss=0) and (a5 is null)) then 1 else 0 end as fill0"
        flag = ",".join([flag_6m, flag_7m, fillNew])
        query = "select *,{0} from dfFlags order by {1}".format(flag, groupBy)
        dfBureauMiss2 = sqlContext.sql(query)
        # Fill sistem
        dfBureauMiss2.createOrReplaceTempView("dfFill")
        hitBureau = "case when (hit_adv=1) and (flagMissing=0) and (flag_cum=0) and (flag_new=0) then 1 else 0 end as hitBureau"
        fill = ",".join(["case when fill0=1 then 0 else {0} end as {0}".format(
            item) for item in aggs["feature"] if item != ""])
        flag = ",".join([groupBy, tfrom, hitBureau, fill])
        query = "select {0} from dfFill order by {1}".format(flag, groupBy)
        dfBureauMiss3 = sqlContext.sql(query)
        return dfBureauMiss3

    def aggregate(self):
        """
        Process to aggregate bureau pull data.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        reading_path = os.path.join(
            self.previous_path, self.agg_dict['table_name'])
        df_in = self.sqlContext.read.parquet(reading_path)
        df_in.createOrReplaceTempView("tempBureau")
        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'])
        df_out = self.sqlContext.sql(query_str)
        df_gcb = self.getCostumerBureau(
            df_out, self.agg_dict['query_dict'], self.sqlContext)
        df_gcb_fin = df_gcb.where("tfrom<=2")
        head, _ = os.path.split(self.previous_path)
        #work_path, prev_step = os.path.split(head)
        self._save_parquet(df_gcb_fin, os.path.join(
            head, "groupby", self.agg_dict['table_name']))


class PNLAggregate:
    """
    Class to aggregate PNL Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _create_groupby_query(self, query_dict, group_by):
        """
        Creates the SQL aggregation sentence to group the PNL data
        
        Parameters
        ----------
        query_dict: Dictionary
            All parameters needed to create SQL sentence
        group_by: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(data=query_dict)
        agg_df = agg_df[agg_df["variable"] != ""]
        features = ",".join(["{0}({1}) as {1}".format(
            item["agg"], item["variable"]) for _, item in agg_df.iterrows()])
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+features + \
            " FROM tempPnl GROUP BY "+group_str
        return query_str

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)

    def aggregate(self):
        """
        Process to aggregate bureau pull data.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        reading_path = os.path.join(
            self.previous_path, self.agg_dict['table_name'])
        df_in = self.sqlContext.read.parquet(reading_path)
        df_in.createOrReplaceTempView("tempPnl")
        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'])
        df_out = self.sqlContext.sql(query_str)
        head, _ = os.path.split(self.previous_path)
        #work_path, prev_step = os.path.split(head)
        self._save_parquet(df_out, os.path.join(head, "groupby"))


class GeneratePivot:
    """
    Class Generate Pivot for transposing a data table

    Parameters
    ----------
    previous_path: string
        Path where data is read.
    pivot_dict: Dictionary
        Parameters needed by the class(key, group_by, etc).
    sqlContext: spark context
        Your sqlContext
    mode: string, optional
        Parquet write mode, defaults to 'overwrite'.

    """

    def __init__(self, previous_path, pivot_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.pivot_dict = pivot_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)

    def pivot_data(self):
        """
        Process to go from long format data to wide format.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        source = self.pivot_dict["source"]
        if source in ("corporate", "clocks", "bureau_pull", "bureau_cohort"):
            read_path = os.path.join(
                self.previous_path, self.pivot_dict["table_name"])
        else:
            read_path = self.previous_path

        df_in = self.sqlContext.read.parquet(read_path)
        group_by = self.pivot_dict["pivot_key"]
        cols, dtypes = zip(*((c, t)
                           for (c, t) in df_in.dtypes if c not in group_by))
        kvs = explode(array([
            struct(lit(c).alias("key"), col(c).alias("value")) for c in cols])).alias("kvs")
        df_long = df_in.select(group_by + [kvs]).select(
            group_by + ["kvs.key", "kvs.value"])
        df_long = df_long.withColumn("tfrom", col("tfrom").cast(IntegerType()))
        df_long = df_long.withColumn("fecha_", concat_ws(
            "_", df_long["key"], df_long["tfrom"]))
        df_wide = df_long.groupBy(
            "numcliente", "vintage_date").pivot("fecha_").avg("value")
        head, _ = os.path.split(self.previous_path)
        #work_path, prev_step = os.path.split(head)
        self._save_parquet(df_wide, os.path.join(head, "pivot"))

    def pivot_data_aux(self, path, group_by):
        """
        Auxiliar function to go from long format data to wide format.

        Parameters
        ----------
        path: string
            Path to read the data from.
        group_by: List
            String list of columns to exclude from the transposing process.

        Returns
        -------
        None

        """
        cols, dtypes = zip(*((c, t)
                           for (c, t) in df_in.dtypes if c not in group_by))
        kvs = explode(array([
            struct(lit(c).alias("key"), col(c).alias("value")) for c in cols])).alias("kvs")
        df_long = df_in.select(group_by + [kvs]).select(
            group_by + ["kvs.key", "kvs.value"])
        df_long = df_long.withColumn("tfrom", col("tfrom").cast(IntegerType()))
        df_long = df_long.withColumn("fecha_", concat_ws(
            "_", df_long["key"], df_long["tfrom"]))
        df_wide = df_long.groupBy(
            "numcliente", "vintage_date").pivot("fecha_").avg("value")
        head, _ = os.path.split(path)
        #work_path, prev_step = os.path.split(head)
        df_wide.write.mode("overwrite").parquet(os.path.join(head, "pivot"))


class CorporateAggregate:
    """
    Class to aggregate Corporate Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class Constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def aggregate(self):
        """
        Process to aggregate corporate data.

        Parameters
        ----------
        None
        :Returns:
        -------
        None
        """
        corporate = self.sqlContext.read.parquet(os.path.join(
            self.previous_path, self.agg_dict["table_name"]))
        conditions = self.agg_dict["conditions"]
        for condition in conditions:
            exec(condition)
        new_columns = self.agg_dict["new_columns"]
        for column in new_columns:
            exec(column)
        drop_columns = self.agg_dict["drop_columns"]
        for column in drop_columns:
            corporate = corporate.drop(column)
        path_save = os.path.join(self.previous_path.replace(
            "extract", "groupby"), self.agg_dict["table_name"])
        self._save_parquet(corporate, path_save)

    def _save_parquet(self, df_save, path):
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data
            
        Returns
        -------
        None
        
        """
        df_save.write.mode(self.mode).parquet(path)


class GroupBy:
    """
    Class Group By for creating the grouped data in datapull.
    
    Parameters
    ----------
    source: string
        Data Source name
    previous_path: string
        Path to save the groupby
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite

    """

    def __init__(self, source, previous_path, agg_dict, sqlContext, mode="overwrite"):
        """
        Class constructor.
        """
        self.source = source
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def run(self):
        """
        Executes groupby process.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        object = self._obtain_object()
        object = object(self.previous_path, self.agg_dict,
                        self.sqlContext, self.mode)
        object.aggregate()

    def _obtain_object(self):
        """
        Returns the corresponding groupby object of the source

        Parameters
        ----------
        None

        Returns
        -------
        object
            Source GroupBy Object
        """
        if self.source == "financiera":
            object = FinancieraAggregate
        elif self.source == "channels":
            object = ChannelsAggregate
        elif self.source == "product_data":
            object = ProductDataAggregate
        elif self.source == "corporate":
            object = CorporateAggregate
        elif self.source == "clocks":
            pass
        elif self.source == "profundidad":
            pass
        elif self.source == "pnl":
            object = PNLAggregate
        elif self.source == "giros_credito":
            object = MacrogirosAggregate
        elif self.source == "bureau_cohort":
            object = BureauCohortAggregate
        elif self.source == "bureau_pull":
            object = BureauPullAggregate
        elif self.source == "liabilities":
            object = LiabilitiesPullAggregate
        elif self.source == "rev_balance":
            object = RevBalanceAggregate
        return object