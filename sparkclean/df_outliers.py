from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col

class OutlierDetector:
	#class for detecting outliers
    def __init__(self, df, column, tolerance = 1.5):
        
        #:param df: dataframe
        #:param column: column to be checked type: string
        #:param tolerance: how much interquantile range the user would like to limit dataset in 
        self.spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
        self._df = df
        self._column = column
        self.tolerance = tolerance
        self.median_val = self.quantile(0.50)
        #interquantile range
        self.q1 = self.quantile(0.25)
        self.q3 = self.quantile(0.75)
        self.iqrange = self.q3-self.q1
        self.rownum  = self._df.count()

    def quantile(self,n):
        
        #:param n: n percentage of quantile n in (0,1]
        #:return the interquantile range of the column
        
        return self._df.approxQuantile(self._column, [n], 0.01)[0]
    def outlier(self):
        
        #:return the range of non-outlier
        distance_range = self.tolerance * (self.q3-self.q1)
            
        return (self.q1-distance_range,self.q3+distance_range)
    def percent_outlier(self):
        """
        :return the percentage of outliers in this column
        """
        lower_bound,upper_bound = self.outlier()#lb and ub
        #calculating the number of outliers in the column
        col = self._column
        outlier_num = self._df.rdd.map(lambda x: (x[col],1)).filter \
        (lambda x: x[0] < lower_bound or x[0] > upper_bound) \
        .count()
        return outlier_num/self.rownum

            
            
        