"""
Project: Missing Treatment (Refactorization) (PEP 8, Python3.6 and OOP)
Developer: Gio Guerrero, Brayan borquez, Gil Rodriguez, Jorge Galicia
Update Date: 2022-11-30
Version: 1.0

Script containing the class and methods for the Missing Treatment in datapull
"""


import os
from pyspark.sql.functions import col, lit, greatest, avg, count, when, avg, first, months_between, trunc, expr, min, max
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
import subprocess


class MissingTreatment:
    """
    Class used for Missing Treatment in datapull

    Description: Fills Values of a dataframe, this class is used to fill values after collect_history or after feature Engineering (MOBs)
    ----------

    Parameters
    ----------
    year: string
        String with the year month to be processes with format YYYYMM
    mt_dict: dictionary
        Dictionary with the variables, methods and sources to be processed
    source: String
        Name of the source to specify paths
    n_mis: int
        Month's number of history 12,24,36,...,etc
    mode:
        How will the parquets be written
    """

    def __init__(self, year, mt_dict, source, n_mis, mode="overwrite"):
        """
        Constructor of missing_treatment class
        ----------
        """
        self.year = year
        self.mt_dict = mt_dict
        self.source = source
        self.n_mis = n_mis
        self.mode = mode

################################################ metodo principal mobs (run_missing_treatment) #####################################
    def run_mt(self, path_mt, tables_mt, path_write, sqlContext):
        """
        This method is the main method to run missing treatment, the objective is to fill missing values using statistics (median,average, moving average, interpolation) or a value given by the user

        Parameters
        ----------
        path_mt: String
            String with the df to read before the last folder
        tables_mt: List
            List of strings to be read "n" subfolders
        path_write: string
            specifying the path where the parquet is going to be stored
        sqlContext: SparkSession

        Returns
        -------
        Spark data frame
            Containing the missing treatment df after the fillings
        """
        df_lst = self.__get_dataframes(path_mt, tables_mt, sqlContext)
        # Iterar sobre las tablas que existan en el directorio
        for item_df, df in enumerate(df_lst):
            # Iterar sobre los metodos de filling values
            for item_method, method in enumerate(self.mt_dict["missing_treatment"]["fill_methods"]["metodo"]):
                grouped = self.mt_dict["missing_treatment"]["fill_methods"]["grouped"][item_method]
                if ("fill_stat" in method):
                    df = self.fill_stat(df, grouped, item_method)
                elif ("fill_value" in method):
                    df = self.fill_value(df, item_method)
                elif ("fill_snap" in method):
                    df = self.fill_snap(df, grouped)
                elif ("fill_historic" in method):
                    df = self.fill_historic(df, grouped, 3)
                elif ("fill_debito" in method):
                    df = self.fill_debito(df, grouped, sqlContext)
            self.__write_parquet(df, os.path.join(
                path_write, self.year, self.source, "missing_treatment"), f"{tables_mt[item_df]}")

################################################ metodo principal mobs (run_mobs)        ###########################################
    def run_mobs(self, tables_path, pathwrite, features_path, feat_list, periodo, sqlContext):
        """
        This method is the main method to calculate the months on books of a cliente, the objective is to fill the features of new clients or the clients which have less than 12 months
        ----------
        Parameters
        ----------        
        tables_path: List
            Path with collect_history for each source ("liabilities"...)
        pathwrite: String
            specifying the path where the parquet is going to be stored
        features_path: string
            String with the path of the original features
        feat_list: tuple
            tuple with the names of the features to imputate or copy
        periodo: int
            period of time for each feature (how the features applied the period (_3m,_4m,...,etc))
        sqlContext: SparkSession
        """
        self.calculate_mobs(tables_path, pathwrite, sqlContext)
        self.previous_fixer(pathwrite, features_path,
                            pathwrite, feat_list, periodo, sqlContext)

################################################ metodos para llenado mobs (Calculate)  ############################################
    def calculate_mobs(self, historic_path, pathwrite, sqlContext=None):
        """
        This method calls the corresponding calculate mobs depending on the sources
        that are received. Then it stores the obtained dataframe into the pathwrite.

        Parameters
        ----------
        historic_path: string
            specifying the path where liabilities collect tables are stored
        pathwrite: string
            specifying the path where the parquet is going to be stored
        sqlContext: SparkSession
        """
        df_full = sqlContext.createDataFrame([], StructType([StructField('numcliente', StringType(), True),
                                                            StructField('vintage_date', StringType(), True)]))
        tables_path = os.path.join(
            historic_path, self.year, "mobs_preparation")
        for source in self.mt_dict["missing_treatment"]["mobs"]["sources"]:
            list_tables = self.__list_tables(source)
            if (source == 'liabilities'):
                df = self.liabilities_mobs(
                    tables_path, list_tables, sqlContext=sqlContext)
            elif (source == 'credit_card'):
                df = self.credit_card_mobs(
                    tables_path, list_tables, sqlContext=sqlContext)
            elif (source == 'bureau'):
                df = self.bureau_mobs(tables_path, list_tables, sqlContext)
            df_full = df_full.join(
                df, on=["numcliente", "vintage_date"], how="fullouter")
        df_full = df_full.withColumn("max_liab_cc", lit(
            greatest(col("mobs_liabilities"), col("mobs_credit_card"))))
        self.__write_parquet(df_full, pathwrite, "calculate_mobs")

    def liabilities_mobs(self, tables_path, table_names, sqlContext=None):
        """
        This method specifies the columns to be selected in the query for the 
        6 liabilities tables and then calls __get_dataframes, __calculate_separated_mobs 
        and mobs_tables_union.

        Parameters
        ----------
        tables_path: string
            specifying the path where liabilities collect tables are stored
        table_names: array
            contains the names of the tables to be used (for liabilities must be an array with 6 names)
        sqlContext: SparkSession

        Returns
        -------
        Spark data frame
            Containing the mobs table
        """
        def mobs_tables_union(dfs_liab):
            """
            This method makes a union of all the data frames received.

            Parameters
            ----------
            dfs_liab: array
                contains the 6 data frames from liabilities historic tables
            Returns
            -------
            Spark data frame
                Containing one data frame with the union of all data frames
            """
            for item in range(len(dfs_liab)):
                if item == 0:
                    base = dfs_liab[item]
                else:
                    liab_aux = dfs_liab[item]
                    base = base.union(liab_aux)

            base = base.groupBy(["numcliente", "vintage_date"]).\
                agg(max("mobs_liabilities").alias("mobs_liabilities"))

            return base

        query = ["bigint(numcliente) as numcliente",
                 "to_date(cast(unix_timestamp(tfrom_date,'yyyy-MM-dd') as timestamp)) as tfrom_date",
                 "to_date(cast(unix_timestamp(fec_apertura,'yyyy-MM-dd') as timestamp)) as fec_apertura",
                 "tfrom", "to_date(cast(unix_timestamp(vintage_date,'yyyy-MM-dd') as timestamp)) vintage_date"]
        calculate_mobs = months_between(
            col("tfrom_date"), trunc(col("fec_apertura"), "month"))
        dfs = self.__get_dataframes(
            tables_path, table_names, sqlContext=sqlContext)
        dfs_liab = self.__calculate_separated_mobs(
            dfs, query, 'liabilities', calculate_mobs)
        base = mobs_tables_union(dfs_liab)
        return base

    def credit_card_mobs(self, tables_path, table_names, sqlContext=None):
        """
        This method specifies the columns to be selected in the query for the pnl table 
        and then calls __get_dataframes, __calculate_separated_mobs and mobs_tables_union.

        Parameters
        ----------
        tables_path: string
            specifying the path where liabilities collect tables are stored
        table_names: array
            contains the name of the table to be used
        sqlContext: SparkSession

        Returns
        -------
        Spark Dataframe
            Containing the mobs table
        """
        query = ["bigint(numcliente) as numcliente", "mob", "tfrom",
                 "to_date(cast(unix_timestamp(vintage_date,'yyyy-MM-dd') as timestamp)) vintage_date"]

        calculate_mobs = col("mob")

        dfs = self.__get_dataframes(
            tables_path, table_names, sqlContext=sqlContext)
        dfs_credit = self.__calculate_separated_mobs(
            dfs, query, 'credit_card', calculate_mobs)
        return dfs_credit[0]

    def bureau_mobs(self, tables_path, list_tables, sqlContext=None):
        """
        Calculate months in bureau of bureau customers.

        Parameters
        ----------
        tables_path: string
            specifying the path where bureau collect tables are stored
        list_tables: list of strings
            List with all the different tables needed (["adv_colocacion_1_t_m"])
        sqlContext: SparkSession

        Returns
        -------
        Spark Dataframe
            With the bureau mobs
        """
        lst_pivot = self.__get_dataframes(
            tables_path, list_tables, sqlContext=sqlContext)
        df_mobs = lst_pivot[0].select(
            "numcliente", "a5", "tfrom", "vintage_date")
        df_mobs = df_mobs.withColumn("mobs", col("a5").cast("int")+col("tfrom").cast("int")).\
            groupBy("numcliente", "vintage_date").agg(max("mobs").alias("mobs_bureau")).\
            fillna(0, subset=["mobs_bureau"])
        return df_mobs

################################################ metodos para llenado mobs (Imputacion) ############################################
    def previous_fixer(self, path_mobs_fe, features_path, write_mobs, feat_list, periodo, sqlContext):
        """
        Description:
        Fills the information for each feature, taking 3,6,9,12 months depending on how many months on books does the client have
        ...

        Parameters
        ----------
        path_mobs: string 
            Path were the mobs table is saved
        features_path: string
            Path of the original features calculated.
        pathwrite: string
            Path where the new features filled will be saved
        feat_list: list
            List of features to which mobs imputation will be applied
        sqlContext: sqlContext
            Your sqlContext.

        """
        mb_ty = self.mt_dict["missing_treatment"]["mobs"]["mobs_tipo"][0]
        path_mobs = os.path.join(path_mobs_fe, "calculate_mobs")
        pathwrite = os.path.join(write_mobs, "imputate_mobs")
        join_mobs = sqlContext.read.parquet(path_mobs).select(
            col('numcliente'), col('vintage_date'), col(mb_ty).alias('mobs'))
        raw_data = sqlContext.read.parquet(os.path.join(features_path, "raw_features")).select(
            col("id_msf_tools_v2_12315"), col("numcliente"), col('vintage_date'))
        join_mobs = raw_data.join(join_mobs, ["numcliente", "vintage_date"], "left").drop(
            "numcliente").drop("vintage_date")
        join_mobs = join_mobs.withColumn("mobs", when(
            col("mobs").isNull(), 0).otherwise(col("mobs")))
        # genera la lista de meses de la periodicidad de los features [3,6,9,12],[2,4,6],...,etc
        lst_months = self.__get_months(periodo)
        df_filter = []
        for feature in feat_list[0]:  # imputacion de features
            print(f"inicia imputacion {feature}")
            months_replace = lst_months[:]
            df_feature_origin = sqlContext.read.parquet(
                os.path.join(features_path, feature+"/"))
            for item in lst_months:
                if (item == periodo):  # clientes nuevos
                    df = self.fill_mobs_0_2(
                        df_feature_origin, join_mobs.filter(f"mobs <= {item-1}"))
                    self.__write_parquet(df, pathwrite, "temp_0")

                    df_filter.append("temp_0")
                else:
                    df = self.fill_months(df_feature_origin, join_mobs.filter(
                        f"mobs >= {(item-periodo)} and mobs <= {item-1}"), months_replace, item-periodo)
                    self.__write_parquet(df, pathwrite, f"temp_{item-periodo}")
                    df_filter.append(f"temp_{item-periodo}")
                months_replace.remove(item)
            df = self.fill_months(df_feature_origin, join_mobs.filter(
                f"mobs >= {item}"), months_replace, item)  # clientes > n_mis meses
            self.__write_parquet(df, pathwrite, f"temp_{item}")
            df_filter.append(f"temp_{item}")
            # Hacer el join de los parquets
            # Se leen los parquets intermedios
            all_df = self.__get_dataframes(pathwrite, df_filter, sqlContext)
            df_imputate = all_df[0]
            for i in range(1, len(all_df)):
                df_imputate = df_imputate.union(all_df[i])
            self.__write_parquet(df_imputate, pathwrite, feature)
            self.__delete_intermediate_list(pathwrite, df_filter)
            df_filter.clear()
        for feature in feat_list[1]:  # Copia de features
            print(f"inicia copia {feature}")
            self.__copy_feature(features_path, feature, pathwrite, sqlContext)
        self.__delete_intermediate_list(pathwrite, df_filter)

    def fill_mobs_0_2(self, df, df_mobs_months):
        """
        Description:
        Method to fill the information with new clients (Less or equal than 2 months on books)

        ...
        Parameters
        ----------
        df: pyspark dataframe 
            Df of the original feature
        df_mobs_months: pyspark dataframe 
            Df with the clients that have less or equal than 2 months.

        Returns
        -------
        Dataframe
            Dataframe with the features imputed
        """
        df_mobs_02 = df_mobs_months.drop("mobs")
        columns = df.columns
        columns.remove("id_msf_tools_v2_12315")
        for column in columns:
            df_mobs_02 = df_mobs_02.withColumn(column, lit(-99999))
        return (df_mobs_02)

    def fill_months(self, df, df_mobs_months, months_list, month_replace):
        """
        Description:
        Method to fill the information with clients that have more than 2 months and less than N months on books
        ...

        Parameters
        ----------
        df: pyspark dataframe 
            Df of the original feature
        df_mobs_months: pyspark dataframe 
            Df month's list that will take the information of the month to replace
        months_list: pyspark dataframe 
            Month's list that will be filled
        month_replace: integer 
            Month that will take the information of the month's list

        Returns
        -------
        Dataframe
            Dataframe with the features imputed
        """
        df_to_fill = df_mobs_months.drop("mobs")
        df_to_fill = df_to_fill.join(
            df, how="inner", on="id_msf_tools_v2_12315")
        columns = df.columns
        columns.remove('id_msf_tools_v2_12315')
        for feature_name in columns:
            for month in months_list:
                if (feature_name.replace(f"_{month}m", f"_{month_replace}m") not in columns):
                    break
                elif (feature_name.endswith(f"_{month}m")):
                    df_to_fill = df_to_fill.withColumn(
                        feature_name, df_to_fill[feature_name.replace(f"_{month}m", f"_{month_replace}m")])
        return (df_to_fill)

########################################################## metodos para llenado de MT ##############################################
    def fill_historic(self, df, grouped, kmiss):
        """
        Description:
        Method to iterate the main dictionary and process the DF acording to the dict
        ...
        Parameters
        ----------
        df: Dataframe
            DF to be processed
        grouped: List
            List of strings with the number of the variables how the dataframe will be grouped
        kmiss: List
            Number of the consecutive missings

        Returns
        -------
        Dataframe
            Dataframe with the info filled
        """
        id_variables = self.mt_dict["missing_treatment"]["fill_methods"]["id_variables"][0]
        wp_client = Window.partitionBy(grouped).orderBy(col("tfrom")).rangeBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)
        wp_client_desc = Window.partitionBy(grouped).orderBy(col("tfrom").desc(
        )).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        wp_client_mid = Window.partitionBy(grouped).orderBy(
            col("tfrom")).rangeBetween(-1, 1)
        wp_mov_avg_b = Window.partitionBy(grouped).orderBy(
            col("tfrom")).rangeBetween(1, kmiss)
        wp_mov_avg_l = Window.partitionBy(grouped).orderBy(
            col("tfrom")).rangeBetween((kmiss)*-1, -1)
        for item, variable in enumerate(self.mt_dict["missing_treatment"]["fill_variables"]["variable"]):
            if (variable in df.columns):  # Si la columna existe en el df, hacer el llenado del missing
                df = df.withColumn("null_count", count(when(df[variable].isNull(), 1)).over(
                    wp_client))  # Conteo de nulos por cliente
                if (self.mt_dict["missing_treatment"]["fill_variables"]["metodo"][item] == id_variables):
                    # casos de interpolacion
                    df = df.withColumn(variable,
                                       when(  # 1 tfrom = 0 tomar el snapshot primer no nulo de cliente
                                           (df.tfrom == 0) & (df.null_count <= 2) & (df[variable].isNull()), first(
                                               df[variable], ignorenulls=True).over(wp_client)
                                           # 2 tfrom es el ultimo mes, se toma snapshot ordenado por cliente de forma descendente
                                       ).when(
                                           (df.tfrom == (int(self.n_mis)-1)) & (df.null_count <= 2) & (
                                               df[variable].isNull()), first(df[variable], ignorenulls=True).over(wp_client_desc)
                                           # 3 tfrom es mayor que cero se toma promedio de lag y lead
                                       ).when(
                                           (df.tfrom > 0) & (df.tfrom < (int(self.n_mis)-1)) & (df.null_count <= 2) & (
                                               df[variable].isNull()), avg(df[variable]).over(wp_client_mid)
                                           # 1 moving average toma la ventana de los kmiss valores siguientes cuando tfrom + kmiss <= (meses-1)
                                       ).when(
                                           (df.null_count == 3) & ((df.tfrom + kmiss) <= (self.n_mis-1)
                                                                   ) & (df[variable].isNull()), avg(df[variable]).over(wp_mov_avg_b)
                                           # 2 moving average toma la ventana de los kmiss valores previos cuando tfrom + kmiss > (meses-1)
                                       ).when(
                                           (df.null_count == 3) & ((df.tfrom + kmiss) > (self.n_mis-1)
                                                                   ) & (df[variable].isNull()), avg(df[variable]).over(wp_mov_avg_l)
                                       ).otherwise(df[variable]))
                df = df.drop("null_count")
        return (df)

    def fill_value(self, df, nmbr_id_variables):
        """
        Description:
        Method to iterate the main dictionary and process the DF acording to the dict

        ...
        Parameters
        ----------
        df: Dataframe
            DF to be processed
        nmbr_id_variables: String
            String with the id, to identify wich variables will be filled in a dictionary

        Returns
        -------
        Dataframe
            Dataframe with the info filled
        """
        id_variables = self.mt_dict["missing_treatment"]["fill_methods"]["valor"][nmbr_id_variables]
        for item, variable in enumerate(self.mt_dict["missing_treatment"]["fill_variables"]["variable"]):
            # validar que la variable del mt_dic exista en el df de origen
            if (variable.lower() in df.columns):
                # Validar que se aplique metodo real number
                if (id_variables.lower() == self.mt_dict["missing_treatment"]["fill_variables"]["metodo"][item].lower()):
                    nmbr_rplce = int(self.mt_dict["missing_treatment"]["fill_variables"]["metodo"][item].lower(
                    ).replace(" ", "").replace("real", ""))
                    df = df.withColumn(variable, when(
                        col(variable).isNull(), nmbr_rplce).otherwise(col(variable)))
        return (df)

    def fill_stat(self, df, grouped, nmbr_id_stat):
        """
        Description:
        Method to iterate the main dictionary and process the DF acording to the dict

        ...
        Parameters
        ----------
        df: string
            DF to be processed
        grouped: List
            List of strings with the number of the variables how the dataframe will be grouped
        nmbr_id_stat: String
            ID of the statistic to be applied, "1" means will applied average, "0" means median

        Returns
        -------
        Dataframe
            Dataframe with the info filled
        """
        id_variables = self.mt_dict["missing_treatment"]["fill_methods"]["id_variables"][0]
        id_stat = self.mt_dict["missing_treatment"]["fill_methods"]["valor"][nmbr_id_stat]
        window_spec = Window.partitionBy(grouped).orderBy("tfrom").rangeBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)
        for item, variable in enumerate(self.mt_dict["missing_treatment"]["fill_variables"]["variable"]):
            # Si se hara el metodo de llenado
            if (self.mt_dict["missing_treatment"]["fill_variables"]["metodo"][item] == id_variables):
                # Si la variable del dic en el df, hacer el llenado del missing
                if (variable in df.columns):
                    if (id_stat == "1"):  # Se aplica promedio (Average)
                        df = df.withColumn(variable, when(col(variable).isNull(), avg(
                            col(variable)).over(window_spec)).otherwise(col(variable))).sort(["tfrom"])
                    else:  # Se aplica mediana
                        percentile = expr(
                            f'percentile_approx({variable}, 0.5)')
                        df = df.withColumn(variable, when(col(variable).isNull(), percentile.over(
                            window_spec)).otherwise(col(variable))).sort(["tfrom"])
        return (df)

    def fill_snap(self, df, grouped):
        """
        Description:
        Method to iterate the main dictionary and process the DF acording to the dict

        ...
        Parameters
        ----------
        df: string
            DF to be processed
        grouped: List 
            List of strings with the number of the variables how the dataframe will be grouped

        Returns
        -------
        Dataframe
            Dataframe after fill the missing values using the snapshot
        """
        id_variables = self.mt_dict["missing_treatment"]["fill_methods"]["id_variables"][0]
        window_spec = Window.partitionBy(grouped).orderBy("tfrom").rangeBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)
        for item, variable in enumerate(self.mt_dict["missing_treatment"]["fill_variables"]["variable"]):
            if (self.mt_dict["missing_treatment"]["fill_variables"]["metodo"][item] == id_variables):
                if (variable in df.columns):
                    df = df.withColumn(variable, when(((df.tfrom == 0) & df[variable].isNull()), first(
                        df[variable], ignorenulls=True).over(window_spec)).otherwise(df[variable]))
        return (df)

    def fill_debito(self, df, grouped, sqlContext):
        """
        Description:
        Method to fill_debito (liabilities)

        ...
        Parameters
        ----------
        df: Dataframe
            DF to be processed
        grouped: List 
            List of strings with the number of the variables how the dataframe will be grouped
        sqlContext: SparkSession

        Returns
        -------
        Dataframe
            Dataframe with the info filled
        """
        exclusiones = self.mt_dict["missing_treatment"]["fill_methods"]["valor"][0]
        # Definimos los limites
        expr_m = min("tfrom").alias("timeWindow")
        min_mob = int(df.groupBy().agg(expr_m).collect()[0][0])
        min_mob = min_mob-1
        expr = max("tfrom").alias("timeWindow")
        max_mob = int(df.groupBy().agg(expr).collect()[0][0])
        cuentas = Window.partitionBy(grouped)
        cancel = df.filter(df.indcancelada == 1)
        cancel = cancel.withColumn(
            "localizador_closed", max("tfrom").over(cuentas))
        cancel = cancel.select("numcliente", "num_contrato",
                               "vintage_date", "localizador_closed").distinct()
        new = df.filter(df.indapertura == 1)
        new = new.withColumn("localizador_new", max("tfrom").over(cuentas))
        new = new.select("numcliente", "num_contrato",
                         "vintage_date", "localizador_new").distinct()
        paso = df.join(cancel, grouped, how="left")
        paso = paso.join(new, grouped, how="left")
        paso1 = paso.withColumn("localizador_new", when(
            col("localizador_new").isNotNull(), col("localizador_new")).otherwise(max_mob))
        paso1 = paso1.withColumn("localizador_closed", when(col(
            "localizador_closed").isNotNull(), col("localizador_closed")).otherwise(min_mob))
        # sdo_capital
        paso1 = paso1.withColumn("MFlg_SDO", when(((paso1.tfrom <= paso1.localizador_new) & (paso1.tfrom > paso1.localizador_closed) & (paso1.sdo_capital.isNotNull())) | (
            (paso1.tfrom > paso1.localizador_new) & (paso1.sdo_capital.isNull())) | ((paso1.tfrom <= paso1.localizador_closed) & (paso1.sdo_capital.isNull() | paso1.sdo_capital.isNotNull())), 0).otherwise(1))

        # Variables selection
        aux_exc = exclusiones[:]
        aux_exc = [el for el in aux_exc if el in df.columns]
        aux_exc.extend(grouped)
        aux_exc = list(set(aux_exc))
        lista_fin = list(set(df.columns)-set(aux_exc))
        paso1.createOrReplaceTempView("df_aux_flags")
        # Fill 0 para bandera=0
        text_fill = "select "+",".join(aux_exc)+"," +\
            ",".join(["case when MFlg_SDO=0 and "+item+" is null then 0 else "+item+" end as "+item for item in lista_fin]) +\
            " from df_aux_flags"
        df_fill = sqlContext.sql(text_fill)
        return (df_fill)
####################################################################################################################################
    # Helper methods
####################################################################################################################################

    def __copy_feature(self, features_path, feature, pathwrite, sqlContext):
        """
        Description:
        Method to copy features that will not be processed

        ...
        Parameters
        ----------
        features_path: string
            String with the path where the original feature is
        feature: string
            string with the feature to be copied
        pathwrite: string 
            String with the path where the feature will be saved
        sqlContext: sqlContext
            Your sqlContext.
        """
        data_feature = sqlContext.read.parquet(
            os.path.join(features_path, feature))
        self.__write_parquet(data_feature, pathwrite, feature)

    def __write_parquet(self, df, path, file_name):
        """
        Function to write a parquet

        Parameters
        ----------
        df: pyspark DataFrame
            Dataframe to write
        path: string
            Path where the parquet is going to be writed
        file_name: string
            Name of the parquet
        """
        df.write.mode(self.mode).parquet(os.path.join(path, file_name))

    def __get_dataframes(self, origin_path, table_names, sqlContext=None):
        """
        This method joins the origin_path with each table name in the table_names
        array as a path, and reads the parquet in each of the paths. 

        Parameters
        ----------
        origin_path: string
                specifying the collect history path 
        table_names: array
                contains the names of the tables to be used (for liabilities must be an array with 6 names)
        sqlContext: SparkSession

        Returns
        -------
        List of Spark data frames
            Containing the tables in the specified paths
        """
        dfs = []
        for name in table_names:
            df = sqlContext.read.parquet(os.path.join(origin_path, name))
            dfs.append(df)
        return dfs

    def __calculate_separated_mobs(self, dfs, query, source, calculate_mobs):
        """
        This method applies the query to each data frame in the dfs array, 
        validates clients and then calculate mobs. 

        Parameters
        ----------
        dfs: array
            contains the data frames where mobs are going to be calculated
        query: array
            contains the string of the columns to be selected
        source: sting
            specifies the mobs' source, is used for the mobs column rename
        calculate_mobs: operation
            contains the operation for calculating the relative mobs

        Returns
        -------
        List of Spark data frames
            Containing the tables in the specified paths
        """
        dfs_liab = []
        validate_clients = col("numcliente") > 0
        calculate_real_mobs = col("mobs").cast(
            'int') + col("tfrom").cast('int')
        for df in dfs:
            df = df.selectExpr(query).\
                filter(validate_clients).\
                withColumn("mobs", calculate_mobs).\
                withColumn("real_mobs", calculate_real_mobs).\
                groupBy("numcliente", "vintage_date").\
                agg(max("real_mobs").alias(f"mobs_{source}")).\
                fillna(0, subset=[f"mobs_{source}"])
            dfs_liab.append(df)
        return dfs_liab

    def __list_tables(self, data_source):
        """
        Description:
        Method to create a list with the tables to read depending the method to calculate

        ...
        Parameters
        ----------
        data_source: String
            Name of the mobs method to calculate (mobs_bureau,mobs_liabilities,max_liab_cc,mobs_credit_card)

        Returns
        -------
        String List
            List with the name of the tables depending the method to calculate e.g: ["adv_colocacion_1_t_m_historic"]
        """
        list_tables = []
        for item, source in enumerate(self.mt_dict["missing_treatment"]["mobs"]["source"]):
            if (source == data_source):
                list_tables.append(
                    self.mt_dict["missing_treatment"]["mobs"]["table_name"][item])
        return (list_tables)

    def __get_months(self, periodo):
        """
        Description:
        Method to create the months period ([3,6,9,12],[2,4,6,...])

        ...
        Parameters
        ----------
        periodo: int
            number of feature's period (3,4,5,..,etc)

        Returns
        -------
        list of integers
            List with the months [3,6,9,12]
        """
        months_list = []
        for month in range(0, self.n_mis, periodo):
            if ((month+periodo) < self.n_mis):
                months_list.append(month+periodo)
            else:
                months_list.append(self.n_mis)
        return (months_list)

    def __evaluate_path(self, path):
        """
        Function to evaluate if a path exists
        Parameters
        ----------
        path: string
            Path to check if exist or not

        Retuns
        -------
        bool
            True if path exist, false otherwise
        """
        args_list = ['hdfs', 'dfs', '-test', '-d', path]
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        proc.communicate()
        value = True if proc.returncode == 0 else False
        return value

    def __delete_intermediate(self, path):
        """
        Function to delete paths using hadoop

        Parameters
        ----------
        path: string
            Path to delete
        """
        if (self.__evaluate_path(path)):
            args_list = ['hdfs', 'dfs', '-rm', '-f', '-r', '-skipTrash', path]
            proc = subprocess.Popen(
                args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            proc.communicate()

    def __delete_intermediate_list(self, path, id_list):
        """
        Function to delete paths using hadoop but using a list with multiple paths

        Parameters
        ----------
        path: string
            Path to delete
        id_list: List of strings
            concatenate the path and each item of the id_list
        """
        for item in id_list:
            self.__delete_intermediate(os.path.join(path, item))