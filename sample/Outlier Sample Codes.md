# Outlier Sample Codes

### Load The Demo DataFrame
~~~python
>>> from pyspark.sql.types import StringType, IntegerType, StructType, StructField
>>> schema = StructType([
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("population", IntegerType(), True)])
>>> countries = ['Colombia', 'US@A', 'Brazil', 'Spain','a','b','c']
>>> cities = ['Bogotá', 'New York', '   São Paulo   ', '~Madrid','d','e','f']
>>> population = [37800000,19795791,12341418,6489162,15000000,999999,100000000]
>>> df = sc.spark.createDataFrame(list(zip(cities, countries, population)), schema=schema)
>>> df.show()
~~~
~~~
+---------------+--------+----------+
|           city| country|population|
+---------------+--------+----------+
|         Bogotá|Colombia|  37800000|
|       New York|    US@A|  19795791|
|   São Paulo   |  Brazil|  12341418|
|        ~Madrid|   Spain|   6489162|
|              d|       a|  15000000|
|              e|       b|    999999|
|              f|       c| 100000000|
+---------------+--------+----------+
~~~

### Detect outliers

~~~python
>>> outl = sparkclean.OutlierDetector(df,'population')
>>> outl.precent_outlier()
0.14285714285714285
>>> tf = sparkclean.DataFrameTransformer(df)
>>> lb,ub = outl.outlier()
~~~

### Delete outliers
~~~python
>>> tf.delete_row((tf.df['population'] > lb) & (tf.df['population'] < ub))
<sparkclean.df_transformer.DataFrameTransformer object at 0x115af9710>
>>> tf.show()
~~~
~~~
+---------------+--------+----------+
|           city| country|population|
+---------------+--------+----------+
|         Bogotá|Colombia|  37800000|
|       New York|    US@A|  19795791|
|   São Paulo   |  Brazil|  12341418|
|        ~Madrid|   Spain|   6489162|
|              d|       a|  15000000|
|              e|       b|    999999|
+---------------+--------+----------+
~~~
