# Integrity Constraint Sample Codes

### Use a json file to define integrety

~~~json
{
    "rules" : [
        {
            "type" : "fd",
            "value" : ["ZIPCode | City, State"]
        }
    ]
}
~~~


### Load The Demo DataFrame
~~~python
>>> #change the path to your files
>>> file_path = '/Users/LinaQiu/Downloads/BigData/project_doc/hospital.csv'
>>> ic_path = '/Users/LinaQiu/Downloads/BigData/project_doc/example.json'
>>> df = spark.read.format('csv').options(header='true',inferschema='true').load(path)
>>> corrector = sparkclean.ICViolationCorrector(df)
>>> corrector.parse_ic(icpath)
<sparkclean.df_ic.ICViolationCorrector object at 0x1069bf320>
>>> corrector.check_violations()
<sparkclean.df_ic.ICViolationCorrector object at 0x1069bf320>
>>> corrector.display_violation_rows()
~~~
~~~
+-------+-----------+-----+-----+                                               
|ZIPCode|       City|State|count|
+-------+-----------+-----+-----+
|  35233| BIRMINGHAM|   AL|   99|
|  35233| BIRMINGHAM|   NY|    1|
|  90028|LOS ANGELES|   CA|   25|
|  90028|  HOLLYWOOD|   CA|   25|
+-------+-----------+-----+-----+

~~~
There're four rows violates the functional dependency

### Correct them

~~~python
>>> corrector.correct_violations('min_cells')
~~~

### Check violations again

~~~python
>>> corrector.check_violations()                                                
<code.df_ic.ICViolationCorrector object at 0x1069c2390>
~~~
~~~
>>> corrector.display_violation_rows()
+-------+-----------+-----+-----+                                               
|ZIPCode|       City|State|count|
+-------+-----------+-----+-----+
|  90028|LOS ANGELES|   CA|   25|
|  90028|  HOLLYWOOD|   CA|   25|
+-------+-----------+-----+-----+

# These two can't be removed because their count are same.
~~~

