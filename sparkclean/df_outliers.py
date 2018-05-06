from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.mllib.clustering import KMeans
from sparkclean.df_transformer import DataFrameTransformer
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import math
import numpy as np

class OutlierDetector:
	#class for detecting outliers
    def __init__(self, df, column, tolerance = 1.5):
        
        #:param df: dataframe
        #:param column: column to be checked type: string
        #:param k: number of klusters the user wants to form
        
        self._df = df
        self._column = column
        self.rownum  = self._df.count()
        self.transformer = DataFrameTransformer(self._df)
        self.transform = False
        
    def normalize(self,col):
        max_column = self._df.agg({col:'max'}).collect()[0][0]
        min_column = self._df.agg({col:'min'}).collect()[0][0]
        def change(value):
            if max_column == min_column:
                return 0
            return (value-min_column)/(max_column-min_column)
        df = self.transformer.df
        udfValue = udf(change,DoubleType())
        df = df.withColumn('new'+col, udfValue(col))
        df = df.drop(col).withColumnRenamed('new'+col,col)
        self.transformer = DataFrameTransformer(df)
        return
        
    def column_datatype(self,normalize = True):
        '''
        : check datatype for dataframe with input columns
        '''
        if isinstance(self._column, str):
            self._column = [self._column]
        number_type = ["DoubleType", "FloatType", "IntegerType","LongType", "ShortType"]
        for c in self._column:
            if c not in self._df.columns:
                print("This column does not exit.")
                return
            else:
                if str(self._df.schema[c].dataType) not in number_type:
                    print('The type of '+c+" is "+str(self._df.schema[c].dataType))
                    print("But we only accept numeric types here.")
                    return
        if normalize:
            for c in self._column:
                self.normalize(c)
        self.transformer.addPrimaryKey()
        self._column.append('id')
        self._df = self.transformer.df
        self.transform = True
        return 
        
    def kmeans_check(self,T,k = 3,normalize = True):
        '''
        #:param: indegree  threshold. if T<1, at least one vector in each group would be removed
        #:return: outlier list
        '''
        if not self.transform:
            self.column_datatype(self._column)
        trans_df = self._df.select(self._column).rdd.map(lambda x : np.array(x))
        clusters = KMeans.train(trans_df.map(lambda x: x[:-1]),k, maxIterations = 10, runs = 1, initializationMode = 'random')
        maxIngroup = trans_df.map(lambda x: (clusters.predict(x[:-1]), \
        np.linalg.norm(clusters.centers[clusters.predict(x[:-1])]-x[:-1]))).reduceByKey(lambda x,y: x if x>y else y).collect()
        maxIngroup = sorted(maxIngroup)
        distForAll = trans_df.map(lambda x: (x[-1],np.linalg.norm(clusters.centers[clusters.predict(x[:-1])]-x[:-1])/ \
        maxIngroup[clusters.predict(x[:-1])][1]))
        outlier_index = distForAll.filter(lambda x: x[1]>T).map(lambda x: int(x[0])).collect()
        print('Around %.2f of rows are outliers.' %(len(outlier_index)/self.rownum))
        self.transform = False
        return outlier_index
   
    def delete_outlier_kmeans(self,T,k = 3,normalize = True):
        #:param T: indegree  threshold in kmeans
        #:delete outliers and return new df
        res = self.kmeans_check(T,k,normalize)
        self.transformer.delete_row(self.transformer.df['id'].isin(res) == False)
        self._df = self.transformer.df
        return
               
    def delete_outlier_one(self,tolerance = 1.5,normalize = True):
        """
        :param tolerance: how much interquantile range the user would like to limit dataset in 
        :delete the row and return new df
        """
        res = self.outlier(tolerance,normalize)
        self.transformer.delete_row(self.transformer.df['id'].isin(res) == False)
        self._df = self.transformer.df
        return
        
    def outlier(self,tolerance = 1.5,normalize = True):
        """
        :param tolerance: how much interquantile range the user would like to limit dataset in 
        :return outlier list
        """
        if isinstance(self._column, str):
            self._column = [self._column]
        else:
            print('This function only works for one column.') 
            return
        if not self.transform:
            self.column_datatype(self._column)
        def quantile(n):
            #:param n: n percentage of quantile n in (0,1]
            #:return quantile number of the column
            return self._df.approxQuantile(self._column[0], [n], 0.01)[0]
        median_val = quantile(0.50)
        q1 = quantile(0.25)
        q3 = quantile(0.75)
        iqrange = q3-q1
        distance_range = tolerance * (q3-q1)
        lower_bound,upper_bound = q1-distance_range,q3+distance_range
        outlier_list = self._df.select(self._column).rdd.map(lambda x: np.array(x)).filter \
        (lambda x: x[0] < lower_bound or x[0] > upper_bound).map(lambda x: int(x[1])).collect()
        outlier_num = len(outlier_list)
        print('Around %.2f of rows are outliers.' %(outlier_num/self.rownum))
        self.transform = False
        return outlier_list
    
    def show(self, n=10, truncate=True,  withId = False):
        if withId:
            return self.transformer.df.show(n, truncate)
        else:
            return self.transformer.df.drop("id").show(n, truncate)

            
            
        