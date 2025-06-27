from pyspark.sql.functions import col
import subprocess
from pyspark.ml.feature import VectorAssembler
import os
import re

class FeaturesVectorAssembler:
    """
    Class Feature Vector Assembler for creating the final vector of the features
    Parameters
    ----------
    :path_features: string
        Path where the features including raw are located
    :save_path: string
        Path to save the va
    :sqlContext: spark context
        Your sqlContext
    :raw_name: string
        Optional, the name of folder of raw features, by default is raw_features
    :mode: string
        Optional, mode of write of parquet, by default overwrite
    :id_column: string
        Optional, name of id column in the features
    """
    def __init__(self, path_features, save_path, sqlContext, raw_name = "raw_features", mode = "overwrite", id_column = "id_msf_tools_v2_12315"):
        self.path_features = path_features
        self.save_path = save_path
        self.sqlContext = sqlContext
        self.raw_name = raw_name
        self.transformations = self._check_files(path_features)
        self.transformations.remove(raw_name)
        if "assembled_features" in self.transformations:
            self.transformations.remove("assembled_features")
        self.mode = mode
        self.id_column = id_column
    def _check_files(self,path):
        """
        Obtain a list with the files of certain path in hadoop.
        Parameters
        ----------
        :path: string
            path to obtain the files
        :Returns:
        -------
        List
            List with all the files in the path
        """
        args_list=['hdfs', 'dfs', '-ls', path]
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        proc.wait()
        #return [re.sub(path+"/", '', str(elem[53:])) for elem in stdout.decode('ascii','ignore').splitlines() if len(elem[53:])>0]
        files = [elem.split("/")[-1] for elem in stdout.decode('ascii','ignore').splitlines()]
        for i in range(0,len(files)-1):
            if "Found" in files[i]:
                files.remove(files[i])
            else:
                pass
        
        return files
    
    def _evaluate_path(self,path):
        """
        Evaluate if certain path exists in hadoop
        Parameters
        ----------
        :path: string
            path to check if exists
        :Returns:
        -------
        Boolean
            True if path exist false if not
        """
        args_list=['hdfs', 'dfs', '-test', '-d', path]
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        proc.communicate()
        value=proc.returncode==0 and True or False
        return value
    def _features_vec_assemb(self):
        """
        Creates the vector using vector assembler of spark with all the features created
        """
        features_to_process = []
        colz = []
        path_assmb = []
        id_column = self.id_column
        for feature in self.transformations:
            features_path = os.path.join(self.path_features,feature)
            if self._evaluate_path(features_path):
                df_fts = self.sqlContext.read.parquet(features_path)
                skip = id_column
                colz_aux = [x for x in df_fts.columns if x not in skip]
                colz.append(colz_aux)
                assembler = VectorAssembler(inputCols = colz_aux, outputCol=feature)
                df_assemb = assembler.transform(df_fts).selectExpr(id_column + " as id_msf_" + feature, feature)
                features_to_process.append((features_path, feature, df_assemb))
        new_meta_dict = self._create_new_meta(colz)
        raw_path = os.path.join(self.path_features, self.raw_name)
        if self._evaluate_path(raw_path):
            pivot = self.sqlContext.read.parquet(raw_path).select(id_column)
            cont = 0
            for feature in features_to_process:
                df_join = feature[2]
                if cont == 0:
                    all_features_df = pivot.alias('a').join(df_join.alias('b'), col("a." + id_column) == col("b.id_msf_" + feature[1]), how='left')
                    all_features_df = all_features_df.drop("id_msf_" + feature[1])
                else:
                    all_features_df = all_features_df.alias('a').join(df_join.alias('b'), col("a." + id_column) == col("b.id_msf_" + feature[1]), how='left')
                    all_features_df = all_features_df.drop("id_msf_" + feature[1])
                cont += 1
            colz_all_assemblers = [x for x in all_features_df.columns if x not in id_column]
            all_assemblers = VectorAssembler(inputCols=colz_all_assemblers, outputCol='features_vector')
            final_features_df = all_assemblers.transform(all_features_df)
            final_features_df = final_features_df.select(id_column, "features_vector")
            ## Change metadata
            old_metadata = final_features_df.schema["features_vector"].metadata
            new_metadata = old_metadata
            for meta in new_metadata["ml_attr"]["attrs"]["numeric"]:
                id_column = meta['idx']
                meta['name'] = new_meta_dict[id_column]
            final_features_df = final_features_df.withColumn("features_vector", col("features_vector")\
                .alias("", metadata = new_metadata))
            final_features_df.write.mode(self.mode).parquet(os.path.join(self.save_path))
        return()
    def _create_new_meta(self, colz):
        """
        Creates the vector using vectorassembler of spark with all the features created
        Parameters
        ----------
        :colz: list
            List of columns to creates the new metadata
        :Returns:
        -------
        Dict
            Dictionary with new metadata
        """
        colz_list = []
        cont = 0
        cont2 = 0
        while cont < len(colz):
            while cont2 < len(colz[cont]):
                colz_list.append(colz[cont][cont2])
                cont2 += 1
            cont += 1
            cont2 = 0
        cont = 0
        dict_colz = {}
        for x in colz_list:
            dict_colz[cont] = x
            cont += 1
        return(dict_colz)
    def run(self):
        """
        Perform the vector assembler.
        """
        self._features_vec_assemb()