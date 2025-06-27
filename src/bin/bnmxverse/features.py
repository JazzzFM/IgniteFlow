"""
Project Feature Engineering (Refactorization) (PEP 8, Python3.6 and OOP)
Developer: Gil Rodriguez & Brayan Borquez
Update Date: 2022-12-27
Version: 1.0

Script containing the class and functions for the feature engineering in datapull
"""

import os
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType,DoubleType,ArrayType,StringType
from multiprocessing.pool import ThreadPool
from cytoolz.functoolz import partial

class FeatureEngineering:
    """
    Class used for feature engineering in datapull

    Parameters
    -----------
        
        df: pyspark dataframe. Dataframe with the output of missing treatment
        path: string. Working path of datapull
        date_id: string. Name of the id to save folder
        source: string. Name of the source to make feature_engineering
        period: int. Number of months for agrupations
        sqlContext: pyspark sqlContext. Your sqlContext
        number_threads: int. Number of threads to use, by default 32
        overwrite: string. Mode of write of parquets, by default overwrite
    
    """
    def __init__(self, df, working_path, period, sqlContext, number_threads = 32, mode="overwrite"):
        self.path = os.path.join(working_path, "features")
        self.sqlContext = sqlContext
        self.period = period
        self.number_threads = number_threads
        self.mode = mode
        self.df = self._create_dataframe(df)
        self.list_mvars = self.get_mvars(period)
        self.lista_metodo = self.get_lista_metodo()

    def create_dataframe(self, df):
        """
        Creates the dataframe for the transformations and the raw variables parquet
        
        Args:
            df: pyspark dataframe. Dataframe with the output of missing treatment

        Returns:
            Pyspark dataframe: Dataframe of all raw variables with null filled
        
        """
        df = self._create_raw_variables(df, self.path)
        df = self._fill_na_zero(df)
        return df
    def get_lista_metodo(self):
        """
        Creates the list of lists of variables for certain methods, this list has all the months of variables, for example, for 6 months of variable a5 returns:
        [[a5_0, a5_1, a5_2, a5_3, a5_4, a5_5]]

        Returns:
            list: List with all months of information of the variable
        
        """
        cols_df = self._obtain_columns(self.df)
        variables = self._obtain_variables(cols_df)
        lista_metodo = list()
        for feature in variables:
            lista_aux = self._filter_columns_feature(cols_df, feature)
            lista_aux_2 = []
            for k in range(len(lista_aux.items())):
                lista_aux_2.append(lista_aux[str(k)])
            lista_metodo.append(lista_aux_2)
        return lista_metodo

    def get_mvars(self, period):
        """
        Makes the list with the variables grouped by months

        Returns:
            list: Variables grouped by months
        """
        cols_df = self._obtain_columns(self.df)
        variables = self.obtain_variables(cols_df)
        list_mvars = self.list_mvars(variables,cols_df,period)
        return list_mvars
    
    def create_raw_variables(self,df,path):
        """
        Creates a parquet with the raw variables
        
        Args:
            df: pyspark dataframe. Dataframe with the output of missing treatment
            path: string. Path to write the features (raw variables for this function)

        Returns:
            pyspark dataframe. Dataframe of all raw variablles
        """
        print("Running raw features")
        df = df.withColumn("id_msf_tools_v2_12315", F.monotonically_increasing_id())
        df.write.mode(self.mode).parquet(path + "/raw_features")
        df = self.sqlContext.read.parquet(path + "/raw_features")
        print("Ending raw features")
        return(df)
    
    def obtain_columns(self, df):
        """
        Make the array with only numerical columns

        Parameters:
            df: pyspark dataframe. Dataframe of raw variables

        """
        cols = df.columns
        col_string = [item[0] for item in df.dtypes if item[1].startswith('double')]
        cols_df = [x for x in cols if x in col_string]
        return cols_df
    
    def filter_columns_feature(self, cols_df, feature):
        """
        Obtaing a list of features in cols of dataframe

        Parameters:
            cols_df: list. List with the columns of dataframe
            feature: string. feature to obtain the list
        
        Returns:
            list: All the features in the columns of dataframe
            
        {'0': 'hitbureau_0', '1': 'hitbureau_1', '10': 'hitbureau_10', '11': 'hitbureau_11', '2': 'hitbureau_2', '3': 'hitbureau_3', '4': 'hitbureau_4', '5': 'hitbureau_5', '6': 'hitbureau_6', '7': 'hitbureau_7', '8': 'hitbureau_8', '9': 'hitbureau_9'}
        
        """
        aux = [str for str in cols_df if feature in str]
        dict_feat = {}
        n = len(feature)
        aux_2 = [variable for variable in aux if variable.rindex("_") == n]
        for feature_aux in aux_2:
            dict_feat[feature_aux[n+1:]] = feature_aux
        return dict_feat
    
    def obtain_variables(self, cols_df):
        """
        Obtain a list with the set of all raw features in dataframe
        
        Args:
            cols_df: list. All numerical columns in the dataframe
        
        Returns:
            list: List of all the disctinct variables in dataframe
        """
        variables = []
        for variable in cols_df:
            n = variable.rindex("_")
            variables.append(variable[:n])
        variables = list(set(variables))
        return variables
    
    def list_mvars(self, variables, cols_df, periodo):
        """
        Obtain the list of variables by month, for example, using 6 months of variable a5  and a period of 2, returns:
        [[a5_0, a5_1],
         [a5_0, a5_1, a5_2, a5_3],
         [a5_0, a5_1, a5_2, a5_3, a5_4, a5_5]]
        
        Example, if periodo = 3 for 12 months of information for a feature returns feature_3m, feature_6m, feature_9m, feature_12m
        
        Args:
            variables: list. List of all unique variables
            cols_df: list. List with the columns of dataframe
            periodo: int. Period to make feature engineering
            
        
        Returns:
            list: grouped months by variable

        """
        return_features = list()
        for feature in variables:
            features = self._filter_columns_feature(cols_df, feature)
            cont = 0
            n_periodo = 0
            while cont < len(features):
                cont += 1
                if cont % periodo == 0:
                    n_periodo += 1
                    return_features.append(
                          [value for key, value in features.items() \
                            if int(key) < n_periodo * periodo])
        return return_features

    def fill_na_zero(self,df):
        """
        Fill the null values of dataframe with zeros

        Args:
            df: pyspark dataframe. Dataframe to fill nulls with zeros

        Returns:
            pyspark dataframe: Dataframe with nulls filled with zeros
        
        """
        df = df.na.fill(0)
        return(df)

    def write_parquet(self,df, path, file_name):
        """
        Write the parquet of a certain df
        
        Parameters
            df: pyspark dataframe. Dataframe to write
            path: string. path to write dataframe
            file_name: string. name of the parquet file
        
        Returns:
            None
        """
        df.write.mode(self.mode).parquet(os.path.join(path, file_name))
        return()

    def run_transformation(self, transformation):
        """
        Run the specified transformation

        Parameters:
            transformation: string. Name of transformation to run
        
        """
        exec("""self._""" + transformation + """Transformation()""")
    
    def run(self, transformations:list) -> None:
        """
        Run the transformations required

        Parameters:
            transformations: list. List with all the transformations
        """
        self.transformations = transformations
        #self.sqlContext.registerDataFrameAsTable(self.df, "df")
        self.df.registerTempTable("df")
        thread_pool = ThreadPool(self.number_threads)
        fun = partial(self._run_transformation)
        trans_dependeces = []
        if "ratiosmod" in transformations:
            transformations.remove("ratiosmod")
            trans_dependeces.append("ratiosmod")
        thread_pool.map(fun, transformations)
        if len(trans_dependeces) > 0:
            thread_pool.map(fun, trans_dependeces)
        thread_pool.close()

    def app_transformation(self, list_vars, key):
        """
        Make the query to do the transformation using the register raw variables dataframe

        Parameters:
            key: list. List with the name of the functions defined previously
            list_vars: list. List of variables to make the transformation, can be lista_metodo or lista_mvars
        
        Returns:
            app_transformation: query used to make the transformation
        """
        if list_vars == "list_mvars":
            aux = ""
            for k in range(len(key)):
                if k == 0:
                    aux = ', '.join([
                          key[k] + "(array(" +\
                          ', '.join(inner_list) +")) as " +\
                          key[k] + "_" +\
                          inner_list[0][:inner_list[0].rindex("_")] +\
                          "_" + str(len(inner_list)) + "m" \
                          for inner_list in self.list_mvars])
                else:
                    aux = aux + ", " + ', '.join([
                          key[k] +\
                          "(array(" +\
                          ', '.join(inner_list) +\
                          ")) as " +\
                          key[k]+"_" +\
                          inner_list[0][:inner_list[0].rindex("_")] +\
                          "_" + str(len(inner_list)) + "m" \
                          for inner_list in self.list_mvars])
            
            app_transformation = "select id_msf_tools_v2_12315, " + aux + " from df"
        
        elif list_vars == "lista_metodo":
            aux = ""
            
            for k in range(len(key)):
                if k == 0:
                    aux = ', '.join(
                            [f"{key[k]}(array(" +\
                            ','.join(inner) +\
                            ")) as " +\
                            f"{key[k]}_" +\
                            str(inner[0][:inner[0].rindex("_")]) \
                            for inner in self.lista_metodo])

                else:
                    aux = aux + ", " + ', '.join([f"{key[k]}(array(" +\
                          ','.join(inner) +")) as " +\
                          f"{key[k]}_" +\
                          str(inner[0][:inner[0].rindex("_")] )\
                          for inner in self.lista_metodo])

            app_transformation = "select id_msf_tools_v2_12315, " + aux + " from df"
        return app_transformation

    def defineUdfTransformation(self, key:list, transformation:list, list_vars:any, tipo:any=DoubleType()):
        """
        Define the udf transformation and apply for the list of variables

        Parameters:
            key: list. List with the name of functions nedeed to apply
            transformation: list. List with the transformations to apply
            list_vars: int. List with the vars to make transformations, it can be list_mvars or lista_metodo
            tipo: pyspark type. Pyspark type used to return in the udf
        
        Returns:
            Pyspark dataframe: Dataframe with the transformation defined maked
        """
        for k in range(len(key)):
            self.sqlContext.udf.register(key[k], transformation[k], tipo)
        app_transformation = self.app_transformation(list_vars,key)
        return_transformation = self.sqlContext.sql(app_transformation)
        return return_transformation

    def runUdfTransformation(self, name:str, key:list, transformation:list, list_vars:str, tipo:any=DoubleType()):
        """
        Run the transformation using one or many udf's

        Parameters:
            name: string. Name of the transformation to perform
            key: list. List for all the names to define udf
            transformation: list. List of all transformations to define udf
            list_vars: string. List of vars to use, can be lista_metodo or list_mvars
            tipo: Pyspark type. Type to return in udf
        """
        print(f"Running {name} transformations")
        df_tmp = self._defineUdfTransformation(key, transformation, list_vars, tipo)
        self._write_parquet(df_tmp, self.path, name + '_features')
        print(f"Ending {name} transformations")
    
    # Transformaciones elementales
    def meanTransformation(self) -> None:
        """
        Calculates the means
        
        .. math::
            \\frac{1}{n} \\sum_{i=0}^{n} x_{i}
            \\sum_{i=1, \ldots, n-1} \\mid x_{i+1}- x_i \\mid

        Using group of <period> months defined in class
        """
        self.runUdfTransformation("mean", 
                  ['mean'],
                  [lambda x: float(np.mean(x))], 
                  "list_mvars", 
                  DoubleType())

    def stdTransformation(self) -> None:
        """
        Calculates the standard deviation using the formula
       
        .. math::
            \\sqrt{\\frac{1}{n} \\sum_{i=1}^{n} (x_{i}-\\mu)^2}

        Where :math:`\\mu` is the mean, using group of <period> months defined in class
        """
        self._runUdfTransformation("std", 
                  ['std'], 
                  [lambda x: float(np.std(x, ddof = 1))], 
                  "list_mvars", 
                  DoubleType())

    def sumTransformation(self) -> None:
        """
        Calculates the minimum
        
        .. math::
            \\sum_{i=1}^{n} x_{i}

        Using group of <period> months defined in class
        """
        self._runUdfTransformation("sum", 
                                  ['sum'], 
                                  [lambda x: float(np.sum(x))], 
                                  "list_mvars", 
                                  DoubleType())

    def minTransformation(self) -> None:
        """
        Calculates the minimum using group of <period> months defined in class
        """
        self._runUdfTransformation("min",
                                  ['min'], 
                                  [lambda x: float(np.min(x))], 
                                  "list_mvars", 
                                  DoubleType())

    def maxTransformation(self) -> None:
        """
        Calculates the maximum using group of <period> months defined in class
        """
        self.runUdfTransformation("max", ['max'], [lambda x: float(np.max(x))], "list_mvars", DoubleType())
    
    def logmeanTransformation(self) -> None:
        """
        Calculates the logarithm of the mean :math:`log(\\mu)` if the mean is higher than 1
        else returns 0, using group of <period> months defined in class .
        """
        def logmean(x):
            mean_value = float(np.mean(x))
            if mean_value>0:
                return float(np.log(mean_value))
            return (-1e3)

        self.runUdfTransformation("logmean", 
                                  ['logmean'], 
                                  [logmean], 
                                  "list_mvars", 
                                  DoubleType())

    def squareTransformation(self) -> None:
        """
        Calculates the square of the mean :math:`\\mu^{2}` using group of <period> months defined in class.
        """
        def square(x):
            mean_value = float(np.mean(x))
            return float(mean_value)**2
        
        self.runUdfTransformation("square", 
                                  ['square'], 
                                  [square], 
                                  "list_mvars",
                                  DoubleType())

    def inverseTransformation(self) -> None:
        """
        Calculates the inverse of the mean :math:`\\frac{1}{\\mu}` using group of period months defined in class.
        If mean equals to zero the returns a high value (1e5)
        """
        
        def inverse(x):
            mean_value = float(np.mean(x))
            if mean_value != 0.0:
                return 1/mean_value
            else:
                return 1e5

        self.runUdfTransformation("inverse",
                                  ['inverse'], 
                                  [inverse], 
                                  "list_mvars", 
                                  DoubleType())

    def trendTransformation(self) -> None:
        """
        Calculates the minimum
        
        .. math::
            \\sum_{i=1}^{n} x_{i}

        using group of <period> months defined in class
        """

        def trend(x):
            n = len(x)
            y = len(x)
            sum_xy = 0.0
            sum_x_sq = 0.0
            sum_x = 0.0
            sum_y = 0.0
            for z in x:
                xy = float(y*z)
                x_sq=float(y*y)
                x = float(y)
                sum_xy += xy
                sum_x_sq += x_sq
                sum_x += x
                sum_y += float(z)
                y-=1
            num = float(((n*sum_xy)-(sum_x*sum_y)))
            den = float(((n*sum_x_sq)-(sum_x*sum_x)))
            if den != 0.0:
                trend = float(num/den)
            else:
                trend = float(num*1e5)
            return(trend)
        
        self.runUdfTransformation("trend",
                                  ['trend'], 
                                  [trend], 
                                  "list_mvars", 
                                  DoubleType())

    def rmeansTransformation(self) -> None:
        """
        Calculates the ratios of means using actual vs previous, for example, for period of 3 months
        and 12 months of information calculates the ratio of the mean of months 4, 5 and 6 and the mean
        of months 1,2 and 3, also calculates the ratio of mean of months  7, 8, 9, 10, 11 and 12 and the mean
        of months 1,2,3,4,5 and 6.
        """

        def rmeans(x,y):
            m_1 = np.mean(x)
            m_2 = np.mean(y)
            if m_2 != 0.0:
                r_mean = float(m_1 / m_2)
            else:
                r_mean = float(m_1 * 1e5)
            return(r_mean)
        
        print("Running rmeans transformation")
        
        n = len(self.lista_metodo[0]) // self.period
        m = 1
        lista_rmeans = []
        
        while 2*m <= n:
            lista_rmeans += [ [self.lista_metodo[k][:m * self.period],
                              self.lista_metodo[k][m * self.period:2 * m *self.period],
                              self.lista_metodo[k][0][:self.lista_metodo[k][0].rindex("_")]]\
                              for k in range(len(self.lista_metodo))]
            m += 1

        self.sqlContext.udf.register("rmeans", rmeans, DoubleType())
        query = "select id_msf_tools_v2_12315," +\
                ",".join(["rmeans(array(" +\
                ",".join(inner_list[0]) +\
                "),array(" + ",".join(inner_list[1]) +\
                ")) as rmeans_" + inner_list[2] + "_" +\
                str(len(inner_list[0])) + "m" \
                for inner_list in lista_rmeans]) +\
                " from df"

        df_r_means = self.sqlContext.sql(query)
        self.write_parquet(df_r_means, self.path, "rmeans_features")
        print("Ending rmeans transformation")
    
    def incrementalTransformation(self) -> None:
        """
        Calculates how incremental is the variable among the group of months using <period> according
        to the formula:
        
        .. math::
            \\sum_{i=1}^{n-1} {(1 \\quad \\text{if} \\quad x_{i+1} > x_{i} \\quad \\text{else} \\quad 0)}

        where :math:`\\mu` is the mean, using group of <period> months defined in class
        """
        def incremental(x):
            x.reverse()
            const_inc=0.0
            for i in range(len(x)-1):
                if x[i+1]<x[i]:
                    const_inc += 1
            return(const_inc)

        print("Running incremental transformations")
        
        n = len(self.lista_metodo[0])
        m = 1
        lista_inc = []
        
        while self.period * m < n-1:
            lista_inc += [self.lista_metodo[k][:self.period * m + 1] \
                            for k in range(len(self.lista_metodo))]
            m += 1

        self.sqlContext.udf.register("incremental", incremental, DoubleType())
        query = "select id_msf_tools_v2_12315, " +\
                ','.join(["incremental(array(" +\
                ','.join(inner_hist) +")) as " +\
                "i" + str(len(inner_hist)-1) + "_" +\
                inner_hist[0][:inner_hist[0].rindex("_")] \
                  for inner_hist in lista_inc])  + " from df"

        df_incremental = self.sqlContext.sql(query)
        self.write_parquet(df_incremental, self.path, "incremental_features")
        print("Ending incremental transformations")
    
    def vpTransformation(self):
        """
        Calculates the mean and the standard deviation of the percental variations of all months defined
        by the next formula
        
        .. math::
            \\frac{x_{i+1}-x_{i}}{x_{i}}

        If the denomitator equals to 0 then the denomitator is replaced by a small value (1e-5)
        """
        
        def means_vp(x):
            list_vp=list()
            t=0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    vp = float((x[t+1]-x[t])/x[t])
                else:
                    vp = float((x[t+1]-x[t])*1e5)
                list_vp.append(vp)
                t+=1
            m = float(np.mean(list_vp))
            return m
        
        def std_vp(x):
            list_vp=list()
            t=0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    vp = float((x[t+1]-x[t])/x[t])
                else:
                    vp = float((x[t+1]-x[t])*1e5)
                list_vp.append(vp)
                t+=1
            sd = float(np.std(list_vp, ddof = 1))
            return sd

        self.runUdfTransformation("vp", 
                                  ['means_vp','std_vp'], 
                                  [means_vp, std_vp], 
                                  "lista_metodo", 
                                  DoubleType())

    def rtTransformation(self):
        """
        Calculates the mean and the standard deviation of the ratios of all months defined
        by the next formula
        
        .. math::
            \\frac{x_{i+1}}{x_{i}}

        If the denomitator equals to 0 then the denomitator is replaced by a small value (1e-5)
        """
        def means_rt(x):
            list_ratios=list()
            t = 0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    ratio = float(x[t+1]/x[t])
                else:
                    ratio = float(x[t+1]*1e5)
                list_ratios.append(ratio)
                t+=1
            m = float(np.mean(list_ratios))
            return m
        
        def std_rt(x):
            list_ratios=list()
            t = 0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    ratio = float(x[t+1]/x[t])
                else:
                    ratio = float(x[t+1]*1e5)
                list_ratios.append(ratio)
                t += 1
            sd = float(np.std(list_ratios, ddof = 1))
            return sd

        self.runUdfTransformation("rt", 
                                  ['means_rt','std_rt'], 
                                  [means_rt, std_rt], 
                                  "lista_metodo", 
                                  DoubleType())

    def soTransformation(self) -> None:
        """
        Calculates stochastic oscilator using groups of months defined by <period> according the formula:
        
        .. math::
            \\frac{\\mu-x_{min}}{x_{max}-x_{min}}

        If the denomitator equals to 0 then the return value is 1.
        """
        def stochastic_oscillator(x):
            media=sum(x)/len(x)
            minimo=min(x)
            maximo=max(x)
            num=(media - minimo)
            den=(maximo - minimo)
            if den != 0.0:
                so = num/den
            else:
                so = 1.0
            return(so)

        self.runUdfTransformation("rt", 
                                  ['so'], 
                                  [stochastic_oscillator], 
                                  "list_mvars", 
                                  DoubleType())

    def avgmovTransformation(self):
        """
        Calculates the movil averages across the group of months defined by
        """
        def avg_aux(x):
            cont=0.0
            num1=0.0
            den1=0.0
            for var in x:
                num1+=float(var)/float(cont+1)
                den1+=1/float(cont+1)
                cont=cont+1
            return(float(num1)/float(den1))

        self.runUdfTransformation("am", 
                                  ['am'], 
                                  [avg_aux], 
                                  "list_mvars", 
                                  DoubleType())

    def ratiosmodTransformation(self) -> None:
        """
        Calculates ratios and deltas between of the combinations of min, max and mean of all variables.
        This transformation need the mean, min and max transformations, so when this transformations doesn't
        was sended it also creates it.
        """
        def rt_mod(x):
            if x[1] != 0.0:
                rt=float((x[0] / x[1]))
            else:
                rt = 0.0
            return rt

        def df_mod(x):
            dif = float((x[0] - x[1]))
            return dif

        print("Running ratiosmod transformation")
        elemental = ["mean", "min", "max"]
        
        # for trans in elemental:
        # if trans not in self.transformations:
        # exec(f"self._{trans}Transformation()")
        # Registramos las udf
        
        self.sqlContext.udf.register("rt_mod", rt_mod, DoubleType())
        self.sqlContext.udf.register("df_mod", df_mod, DoubleType())
        trans_rt = [("min","max"),("min","mean"),("max","mean")]
        trans_d = [("max","min"),("mean","min"),("max","mean")]
        
        for tran_rt in trans_rt:
            name_ratio = tran_rt[0] + tran_rt[1]
            df_num = self.sqlContext.read.parquet(os.path.join(self.path,tran_rt[0]+"_features"))
            df_den = self.sqlContext.read.parquet(os.path.join(self.path,tran_rt[1]+"_features"))
            list_num = df_num.drop("id_msf_tools_v2_12315").columns
            list_den = df_den.drop("id_msf_tools_v2_12315").columns
            list_rnumden = list()

            for var_num in list_num:
                var_name = var_num[var_num.index("_"):]
                var_den = tran_rt[1] + var_name
                
                if var_den in list_den:
                    list_rnumden.append([var_num, var_den])
            df = df_num.join(df_den,
                            df_num.id_msf_tools_v2_12315 == df_den.id_msf_tools_v2_12315)\
                        .drop(df_den.id_msf_tools_v2_12315)
            
            app_rt_mod_rnumden = "select id_msf_tools_v2_12315, " +\
                                  ', '.join(["rt_mod(array(" +\
                                  ','.join(inner_rnumden) +")) as " +\
                                  f"r{name_ratio}_" +\
                                  inner_rnumden[0][inner_rnumden[0].index("_")+1:]\
                                  for inner_rnumden in list_rnumden]) +\
                                  " from df_aux_r" +\
                                  name_ratio

            self.sqlContext.registerDataFrameAsTable(df, "df_aux_r" + name_ratio)
            df_aux = self.sqlContext.sql(app_rt_mod_rnumden)
            self._write_parquet(df_aux, self.path, f"r{name_ratio}_features")

        for tran_d in trans_d:
            name_d = tran_d[0] + tran_d[1]
            df_num = self.sqlContext.read.parquet(os.path.join(self.path,tran_d[0]+"_features"))
            df_den = self.sqlContext.read.parquet(os.path.join(self.path,tran_d[1]+"_features"))
            list_p = df_num.drop("id_msf_tools_v2_12315").columns
            list_m = df_den.drop("id_msf_tools_v2_12315").columns
            list_dpm = list()
            for var_p in list_p:
                var_name = var_p[var_name.index("_"):]
                var_m = tran_d[1] + var_name
                
                if var_m in list_m:
                    list_dpm.append([var_p, var_m])
            
            df = df_num.join(df_den, 
                          df_num.id_msf_tools_v2_12315 == df_den.id_msf_tools_v2_12315)\
                        .drop(df_den.id_msf_tools_v2_12315)

            app_d_mod_dpm = "select id_msf_tools_v2_12315, " +\
                            ', '.join(["rt_mod(array(" +\
                            ','.join(inner_dpm) +\
                            ")) as " + f"d{name_d}_" +\
                            inner_dpm[0][inner_dpm[0].index("_")+1:] \
                            for inner_dpm in list_dpm]) +\
                            " from df_aux_d" +\
                            name_d

            self.sqlContext.registerDataFrameAsTable(df, "df_aux_d" + name_d)
            df_aux = self.sqlContext.sql(app_d_mod_dpm)
            self.write_parquet(df_aux, self.path, f"d{name_d}_features")
        print("Ending ratiosmod transformation")
    
    def rmsTransformation(self):
        """
        Calculates root mean square using groups of months defined by <period> according the formula:
        
        .. math::
            \\sqrt{\\frac{1}{n} \\sum_{i=1}^{n} x_{i}^2}
        
        """
        def rms(x):
            return float(np.sqrt(np.mean(np.square(x))))
        
        self.runUdfTransformation("rms", 
                                  ['rms'], 
                                  [rms], 
                                  "list_mvars", 
                                  DoubleType())

    def cmpxTransformation(self):
        """
        Calculates complexity of all months using the formula:
        
        .. math::
            \\sqrt{\\sum_{i=1}^{n} (x_{i+1}-x_{i})^2}
        """
        def complexity(x):
            x = np.diff(x)
            return float(np.sqrt(np.dot(x, x)))
        
        self.runUdfTransformation("cmpx", 
                                  ['cmpx'], 
                                  [complexity], 
                                  "lista_metodo", 
                                  DoubleType())

    def skewTransformation(self):
        """
        Returns the sample skewness across all the months(calculated with the adjusted Fisher-Pearson
        standardized moment coefficient G1)
        """
        def skewness(x):
            x = pd.Series(x)
            return float(pd.Series.skew(x))
        
        self.runUdfTransformation("skew", 
                                  ['skew'], 
                                  [skewness], 
                                  "lista_metodo", 
                                  DoubleType())

    def curtTransformation(self):
        """
        Returns the kurtosis of all months (calculated with the adjusted Fisher-Pearson standardized
        moment coefficient G2)
        """
        def curtosis(x):
            x = pd.Series(x)
            return float(pd.Series.kurtosis(x))

        self.runUdfTransformation("curt", 
                                  ['curt'], 
                                  [curtosis], 
                                  "lista_metodo", 
                                  DoubleType())
