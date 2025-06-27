"""
Project: ttdproccessed (Refactorization) (PEP 8, Python3.6 and OOP)
Developer: Gio Guerrero, Brayan borquez, Gil Rodriguez, Jorge Galicia
Update Date: 2022-02-17
Version: 1.0

Script containing the class and methods for the processed in datapull
"""


import os
from pyspark.sql.functions import col, lit, when, max, month, year
from pyspark.sql.types import DoubleType
class TTDProcessed:
	def __init__(self,path_write,path_pivot,dict,sql_context,mode="overwrite"):
		"""
		Constructor of missing_treatment object
		----------
		:self: object itself
			String with the df to read before the last folder
		:year: string
			String with the year month to be processes with format YYYYMM
		:dict: dictionary
			Dictionary with the variables, methods and sources to be processed
		:source: String
			Name of the source to specify paths
		:mode:
			How will the parquets be written
		"""
		self.path_write=path_write
		self.path_pivot=path_pivot
		self.dict=dict
		self.mode = mode
		self.sql_context=sql_context
		

	def create_cols(self):
		#extraer anio y mes
		libname=self.dict["group_by"]["libname"][0]
		table_name=self.dict["group_by"]["table_name"][0]
		table=f"{libname}.{table_name}"
		pivot = self.sql_context.read.parquet(self.path_pivot).select("numcliente","vintage")
		aux_tdml = self.sql_context.table(table)
		last_update_tdml = str(aux_tdml.select(max("proc_dt")).collect()[0]["max(proc_dt)"])[:10]
		tdml = aux_tdml.where("proc_dt = '"+ last_update_tdml+"'")
		pivot=pivot.withColumn("vtg",(year(pivot.vintage)*100+month(pivot.vintage)))
		tdml=tdml.select("num_cliente","falta")
		tdml=tdml.withColumn("flag_cruza",lit(1))
		tdml=tdml.withColumn("alta",(year(tdml.falta)*100+month(tdml.falta)))
		final=pivot.join(tdml,tdml.num_cliente==pivot.numcliente,"left")
		# Casos raros: 1 Raro 0 Normal
		final=final.withColumn("aux_date",when(col("alta")>col("vtg"),1).otherwise(0))
		#Calculo
		final=final.withColumn("yob", when(col("aux_date")==0,year(col("vintage"))-year(col("falta"))).otherwise(-2))
		final=final.withColumn("yob_",when(col("aux_date")==0,month(col("vintage"))-month(col("falta"))).otherwise(-2))
		final=final.withColumn("mob", when(col("aux_date")==0,(col("yob")*12)+col("yob_")).otherwise(-2))
		# Missing Treatment
		final=final.withColumn("mob",when(col("falta").isNull(),-1).otherwise(col("mob")))
		final=final.withColumn("yob",when(col("falta").isNull(),-1).otherwise(col("yob")))
		final=final.withColumn("yob",col("yob").cast(DoubleType()))
		final=final.withColumn("mob",col("mob").cast(DoubleType()))
		final=final.withColumnRenamed("vintage","vintage_date")
		# Final select
		final_=final.select("numcliente","vintage_date","mob","yob").distinct()
		self.__write_parquet(final_,self.path_write,"groupby")

	def __write_parquet(self, df, path, file_name):
		"""
		Function to write a parquet

		Parameters
		----------
		:self: Object itself 
		df: pyspark DataFrame
			Dataframe to write
		path: string
			Path where the parquet is going to be writed
		file_name: string
			Name of the parquet
		"""
		df.write.mode(self.mode).parquet(os.path.join(path,file_name))