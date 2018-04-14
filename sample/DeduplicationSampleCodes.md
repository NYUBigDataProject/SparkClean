# Deduplication Sample codes

## Field Matching: Key Collision Clustering

### Load The Demo DataFrame
~~~python
>>> from pyspark.sql.types import StringType, IntegerType, StructType, StructField
>>> def loadDemo():
        schema = StructType([
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("population", IntegerType(), True)])
        countries = ['Colombia', 'America', 'Brazil', 'Spain', 'America', 'Amarica','Colombia']
        cities = ['Bogota', 'New York', '   São Paulo   ', '~Madrid', 'New York', 'New York  ','Bogotá']
        population = [37800000,9795791,12341418,6489162,19795791,19795791,37800000]
        # Dataframe:
        df = sparkclean.spark.createDataFrame(list(zip(cities, countries, population)), schema=schema)
        return df
 
>>> df = loadDemo()

2018-04-14 15:49:11 WARN  ObjectStore:568 - Failed to get database global_temp, returning NoSuchObjectException
>>> de = sparkclean.DataFrameDeduplicator(df)
>>> de._tf.show()
~~~
~~~
+---------------+--------+----------+-----------+                               
|           city| country|population|         id|
+---------------+--------+----------+-----------+
|         Bogota|Colombia|  37800000|          0|
|       New York| America|   9795791| 8589934592|
|   São Paulo   |  Brazil|  12341418| 8589934593|
|        ~Madrid|   Spain|   6489162|17179869184|
|       New York| America|  19795791|17179869185|
|     New York  | Amarica|  19795791|25769803776|
|         Bogotá|Colombia|  37800000|25769803777|
+---------------+--------+----------+-----------+

~~~

### Clustering and Preview

~~~python
>>> colName, clusters = de.keyCollisionClustering("city")
>>> de.preview(clusters, 2)
~~~
~~~
------------ Cluster 0 -------------------
Name: Bogota, Count:1
Name: Bogotá, Count:1
-----------------------------------------------
Will be changed to "Bogota", takes 1/2
------------ Cluster 1 -------------------
Name: New York, Count:2
Name: New York  , Count:1
-----------------------------------------------
Will be changed to "New York", takes 2/3

~~~

Note that you can also apply changes to the clusters before resolving them in any way you like since they are RDDs.

### Resolve changes 

~~~python
>>> de.resolve(colName, clusters)
Total rows affected: 5 rows
>>> de._tf.show()
~~~
~~~
+---------------+--------+----------+-----------+
|           city| country|population|         id|
+---------------+--------+----------+-----------+
|         Bogota|Colombia|  37800000|          0|
|       New York| America|   9795791| 8589934592|
|   São Paulo   |  Brazil|  12341418| 8589934593|
|        ~Madrid|   Spain|   6489162|17179869184|
|       New York| America|  19795791|17179869185|
|       New York| Amarica|  19795791|25769803776|
|         Bogota|Colombia|  37800000|25769803777|
+---------------+--------+----------+-----------+
~~~
