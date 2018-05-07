# SparkClean: A Scalable DataClean Library for PySpark

---
## Requirements

- Python >= 3.4
- Apache Spark >= 2.2.0

---

## Usage

~~~shell
>>> git clone https://github.com/NYUBigDataProject/SparkClean.git
>>> cd SparkClean
>>> pip3 install -r requirements.txt
>>> pyspark
~~~

Then in the Pyspark Shell
~~~python
import sparkclean
~~~

---

## Architecture

1. **Transformer**: Transform DataFrame and Break linage
2. **Deduplicator**: Cluster and Remove Duplications
3. **OutlierDetector**: Find the outliers
4. **IntegrityConstraint Corrector**: Check Integrity Constraints

---


## Transformer

Transform and Break linage

1. Originally from Optimus. Fixed codes for fitting Python3.4
2. Use checkpoints for breaking lineage
3. Provide lots of useful functions

---

## Transformer: Utilities and Break Linage 

~~~
>>> tf = sparkclean.DataFrameTransformer(df)
>>> tf.show()
+------------+--------+----------+                                              
|        city| country|population|
+------------+--------+----------+
|      Bogota|Colombia|  37800000|
|    New York| America|   9795791|
|   São Paulo|  Brazil|  12341418|
|     ~Madrid|   Spain|   6489162|
|    New York| America|  19795791|
|  New York  | Amarica|  19795791|
|      Bogotá|Colombia|  37800000|
+------------+--------+----------+
>>> tf.remove_special_chars(columns=['city','country'])
                        \.clear_accents(columns='*')
>>> tf.show()
+------------+--------+----------+
|        city| country|population|
+------------+--------+----------+
|      Bogota|Colombia|  37800000|
|    New York| America|   9795791|
|   Sao Paulo|  Brazil|  12341418|
|      Madrid|   Spain|   6489162|
|    New York| America|  19795791|
|  New York  | Amarica|  19795791|
|      Bogota|Colombia|  37800000|
+------------+--------+----------+
~~~

---


## Deduplicator

Clustering and Remove Duplications

1. Fingerprint clustering 
2. Locality-Sensitive Hashing clustering
3. Support all kinds of similarity  (Edit,Token,Sequence,Phonetic, by intergating "TextDistance") 
4. Column-wise Deduplication
5. Optimiezed transformatons


---

## Deduplicator: Clustering By Fingerprint

~~~
>>> de = sparkclean.DataFrameDeduplicator(df)
>>> col,cluster = de.keyCollisionClustering("city")
>>> de.preview(col,cluster)
------------ Cluster 0 -------------------                                      
colName: city| Item: New York, Count:2
colName: city| Item: New York  , Count:1
colName: city|  Will be changed to "New York", takes 2/3
>>> de.resolve(col,cluster)
Total rows affected: 3 rows
>>> de.show()
+------------+--------+----------+
|        city| country|population|
+------------+--------+----------+
|      Bogota|Colombia|  37800000|
|    New York| America|   9795791|
|   Sao Paulo|  Brazil|  12341418|
|      Madrid|   Spain|   6489162|
|    New York| America|  19795791|
|    New York| Amarica|  19795791|
|      Bogota|Colombia|  37800000|
+------------+--------+----------+
~~~

---

## Deduplicator: Clustering By Similarity

~~~
>>> # Nothing showed up, because the threshold is large 
>>> col,cluster = de.localitySensitiveHashing("country", 
            \blockSize=2, method = "levenshtein", threshold = 0.9)
>>> de.preview(col,cluster)

>>> # Find a cluster, because the threshold is smaller
>>> col,cluster = de.localitySensitiveHashing("country", 
            \blockSize=2, method = "levenshtein", threshold = 0.8)
>>> de.preview(col,cluster)
------------ Cluster 0 -------------------
colName: country| Item: America, Count:2
colName: country| Item: Amarica, Count:1
colName: country|  Will be changed to "America", takes 2/3

>>> de.resolve(col,cluster)
>>> de.show()
+------------+--------+----------+
|        city| country|population|
+------------+--------+----------+
|      Bogota|Colombia|  37800000|
|    New York| America|   9795791|
|   Sao Paulo|  Brazil|  12341418|
|      Madrid|   Spain|   6489162|
|    New York| America|  19795791|
|    New York| America|  19795791|
|      Bogota|Colombia|  37800000|
+------------+--------+----------+
~~~

---

## Deduplicator: Record Matching

~~~
>>> col,cluster=de.recordMatching(matchColNames=["city","country"], 
                  \ fixColNames=["population"])
>>> de.previewReords(col,cluster)
------------ Cluster 0 -------------------
Record id: [8589934592, 17179869185, 25769803776]
colName: population| Item: 19795791, Count:2
colName: population| Item: 9795791, Count:1
colName: population| Change: Will be changed to "19795791", takes 2/3

>>> de.resolveRecords(col,cluster)
Total rows affected: 3 rows
>>> de.show()
+------------+--------+----------+
|        city| country|population|
+------------+--------+----------+
|      Bogota|Colombia|  37800000|
|    New York| America|  19795791|
|   Sao Paulo|  Brazil|  12341418|
|      Madrid|   Spain|   6489162|
|    New York| America|  19795791|
|    New York| America|  19795791|
|      Bogota|Colombia|  37800000|
+------------+--------+----------+
~~~

---

## Deduplicator: Deduplication

~~~
>>> de.remove_duplicates(['city','country','population'])
>>> de.show()
+------------+--------+----------+
|        city| country|population|
+------------+--------+----------+
|    New York| America|  19795791|
|   Sao Paulo|  Brazil|  12341418|
|      Bogota|Colombia|  37800000|
|      Madrid|   Spain|   6489162|
+------------+--------+----------+
~~~



---


## IntegrityConstraints Corrector

Check and Correct Integrity Constraints

1. Reasonably fix violations of functional dependencies using a greedy algorithm
2. Do an exploration on the interaction between two or more FDs (holistic data repairing)




---


## IntegrityConstraints Corrector: FD Correction

~~~
>>> transformer = sparkclean.DataFrameTransformer(df)
>>> corrector = sparkclean.ICViolationCorrector(transformer)
>>> corrector.parse_ic('example.json')
>>> corrector.check_violations()
~~~

---

## IntegrityConstraints Corrector: FD Correction

~~~javascript
{
  "rules" :
  [ 
    {"type" : "fd", "value" : 
        ["ZIPCode | City, State"]},
    {"type" : "fd", "value" : 
        ["ProviderNumber | City, PhoneNumber"]}
  ]
}
~~~

---

## IntegrityConstraints Corrector: FD Correction

~~~
>>> corrector.display_violation_rows()

['ZIPCode']  |  ['City', 'State']
+-------+-----------+-----+-----+                                               
|ZIPCode|       City|State|count|
+-------+-----------+-----+-----+
|  35233|    NewYork|   AL|    1|
|  35233| BIRMINGHAM|   AL|   99|
|  90028|LOS ANGELES|   CA|   25|
|  90028|  HOLLYWOOD|   CA|   25|
+-------+-----------+-----+-----+

['ProviderNumber']  |  ['City', 'PhoneNumber']
+--------------+-----------+-----------+-----+                                  
|ProviderNumber|       City|PhoneNumber|count|
+--------------+-----------+-----------+-----+
|       10018.0| BIRMINGHAM| 2053258100|   24|
|       10018.0|    NewYork|  253258100|    1|
|       50063.0|LOS ANGELES| 2134133001|    2|
|       50063.0|LOS ANGELES| 2134133000|   23|
+--------------+-----------+-----------+-----+
~~~



## IntegrityConstraints Corrector: FD Correction

~~~
>>> corrector.correct_violations('single_fd_greedy')
Modify:                                                                         
+-------+-------+-----+
|ZIPCode|   City|State|
+-------+-------+-----+
|  35233|NewYork|   AL|
+-------+-------+-----+

To:
+-------+----------+-----+
|ZIPCode|      City|State|
+-------+----------+-----+
|  35233|BIRMINGHAM|   AL|
+-------+----------+-----+
~~~

---

## IntegrityConstraints Corrector: FD Correction


~~~
This violation cannot be fixed by single_fd_greedy rule                         
+-------+-----------+-----+-----+
|ZIPCode|       City|State|count|
+-------+-----------+-----+-----+
|  90028|LOS ANGELES|   CA|   25|
|  90028|  HOLLYWOOD|   CA|   25|
+-------+-----------+-----+-----+
~~~

---

## IntegrityConstraints Corrector: FD Correction

~~~
Modify:                                                                         
+--------------+-------+-----------+
|ProviderNumber|   City|PhoneNumber|
+--------------+-------+-----------+
|       10018.0|NewYork|  253258100|
+--------------+-------+-----------+

To:
+--------------+----------+-----------+
|ProviderNumber|      City|PhoneNumber|
+--------------+----------+-----------+
|       10018.0|BIRMINGHAM| 2053258100|
+--------------+----------+-----------+
~~~
---


## IntegrityConstraints Corrector: FD Correction

~~~

['ZIPCode']  |  ['City', 'State']
+-------+-----------+-----+-----+                                               
|ZIPCode|       City|State|count|
+-------+-----------+-----+-----+
|       |           |     |     |
|  90028|LOS ANGELES|   CA|   25|
|  90028|  HOLLYWOOD|   CA|   25|
+-------+-----------+-----+-----+

['ProviderNumber']  |  ['City', 'PhoneNumber']
+--------------+-----------+-----------+-----+                                  
|ProviderNumber|       City|PhoneNumber|count|
+--------------+-----------+-----------+-----+
|              |           |           |     |
|              |           |           |     |
|              |           |           |     |
|              |           |           |     |
+--------------+-----------+-----------+-----+

~~~


---

## IntegrityConstraints Corrector: FD Correction

~~~
>>> corrector.correct_violations('holistic')

[Row(array(City, count)=['BIRMINGHAM', '99']), Row(array(City, count)=['NewYork', '1'])]
Field City with value NewYork of row 0 may be wrong
Suggested values are: BIRMINGHAM

>>> corrector.check_violations() 
>>> corrector.display_violation_rows()

~~~

---

## IntegrityConstraints Corrector: FD Correction

~~~
['ZIPCode']  |  ['City', 'State']
+-------+-----------+-----+-----+                                               
|ZIPCode|       City|State|count|
+-------+-----------+-----+-----+
|  90028|LOS ANGELES|   CA|   25|
|  90028|  HOLLYWOOD|   CA|   25|
+-------+-----------+-----+-----+

['ProviderNumber']  |  ['City', 'PhoneNumber']
+--------------+-----------+-----------+-----+                                  
|ProviderNumber|       City|PhoneNumber|count|
+--------------+-----------+-----------+-----+
|       10018.0| BIRMINGHAM|  253258100|    1|
|       10018.0| BIRMINGHAM| 2053258100|   24|
|       50063.0|LOS ANGELES| 2134133001|    2|
|       50063.0|LOS ANGELES| 2134133000|   23|
+--------------+-----------+-----------+-----+
~~~


---


## OutlierDetector

Check and Correct Outlier

1. Check outliers - one column or multiple columns 
2. Normalize - higher accuracy
3. Statistics  report - user friendly 



---


## OutlierDetector: Only For One Column

~~~
>>> df.show()
+----------+--------+----------+----------+
|   country|    city|population|averageGDP|
+----------+--------+----------+----------+
|    Bogotá|Colombia|  37800000|     140.9|
|  New York|     USA|  19795791| 1547100.0|
| São Paulo|  Brazil|  12341418|  450000.0|
|    Madrid|   Spain|   6489162|  338090.0|
|     Kyoto|   Japan|  25430000| 5970000.0|
|Manchester|      UK|   2550000|   74398.0|
|    Cannes|  France|     73603|   36857.0|
|     Milan|   Italy|   3196825|  241200.0|
|    Weimar| Germany|     66000|    1600.7|
+----------+--------+----------+----------+
~~~

---

## OutlierDetector: Only For One Column

~~~
>>> outlier1 = sparkclean.OutlierDetector(df,'averageGDP')
>>> outlier1.delete_outlier_one()
Around 0.22 of rows are outliers.
>>> outlier1.show(normalize_keep = False)
+----------+--------+----------+----------+
|   country|    city|population|averageGDP|
+----------+--------+----------+----------+
|  New York|     USA|  19795791| 1547100.0|
| São Paulo|  Brazil|  12341418|  450000.0|
|    Madrid|   Spain|   6489162|  338090.0|
|     Kyoto|   Japan|  25430000| 5970000.0|
|Manchester|      UK|   2550000|   74398.0|
|    Cannes|  France|     73603|   36857.0|
|     Milan|   Italy|   3196825|  241200.0|
+----------+--------+----------+----------+
~~~

---


## OutlierDetector: For Multiple Columns 

~~~
>>> outlier2 = sparkclean.
                  \OutlierDetector(df,['population','averageGDP'])
>>> outlier2.delete_outlier_kmeans(0.9,normalize_keep = False)
Around 0.44 of rows are outliers.
>>> outlier2.show(normalize_keep = False)
+----------+-------+----------+----------+
|   country|   city|population|averageGDP|
+----------+-------+----------+----------+
|    Madrid|  Spain|   6489162|  338090.0|
|Manchester|     UK|   2550000|   74398.0|
|    Cannes| France|     73603|   36857.0|
|     Milan|  Italy|   3196825|  241200.0|
|    Weimar|Germany|     66000|    1600.7|
+----------+-------+----------+----------+
~~~

---

## Cluster VS Local 

- 136 MB File, 372415 rows, preview clustering on one column
- Sparkclean on AWS 5-core-cluster, Openrefine on macbook air

![](https://s3.amazonaws.com/sparkclean-test/scVSrefince.png)

---
## Optimizationed Transformations 

- 136 MB File, 372415 rows, fix clustering on one column
- Both on AWS 20-core-cluster

![](https://s3.amazonaws.com/sparkclean-test/Optimization.png)


---


## Large Scale Test

- 2.09 GB File, 10803028 Rows, Fix clustering

- OpenRefine ：More the 1 hour to load, Memory Issue , on Laptop

- SparkClean ：Load + fix within 3 minutes , on 20-core-cluster on AWS
