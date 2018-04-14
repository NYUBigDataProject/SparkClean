# Importing sql types
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType, StructType, StructField
# Importing sql functions
from pyspark.sql.functions import col, udf, trim, lit, format_number, months_between, date_format, unix_timestamp, \
    current_date, abs as mag
import pyspark.sql.dataframe

import textdistance
from sparkclean import DataFrameTransformer
import re, string
from unidecode import unidecode
from collections import Counter


class DataFrameDeduplicator:
    """DataFrameDeduplicater is a class to deduplicate in dataFrames"""
    def __init__(self, df):
        self._tf = DataFrameTransformer(df)
        self._tf.addPrimaryKey()
    def keyCollisionClustering(self, colName, method = "levenshtein"):
        self._tf._assert_cols_in_df(columns_provided=[colName], columns_df=self._tf._df.columns)
        rdd = self._tf._df.select(["id", colName]).rdd.map(list)
        def getFingerPrint(s):
            PUNCTUATION = re.compile('[%s]' % re.escape(string.punctuation))
            # preprocess
            preprocessed = PUNCTUATION.sub('', s.strip().lower())
            # unique_preserving_order
            seen = set()
            seen_add = seen.add
            unique = [x for x in preprocessed if not (x in seen or seen_add(x))]
            # latinize 
            latinized = unidecode(''.join(unique))
            return latinized
        def fingerPrintMapper(x):
            _id, _col = x 
            _s = str(_col)
            fingerPrint = getFingerPrint(_s)
            return (fingerPrint, [_id, _col])
        def previewMapper(l):
            fingerPrintMapper, raws = l
            words = list(map(lambda x:x[1], raws))
            ids = list(map(lambda x:x[0], raws))
            d = Counter(words)
            info = (d.most_common(1)[0][0],d.most_common(1)[0][1],len(raws))
            return (fingerPrintMapper, info, d, ids)

        clusters = rdd.map(fingerPrintMapper).groupByKey().mapValues(list).filter(lambda x:len(x[1])>1)
        objects = clusters.map(previewMapper)
        return colName, objects
    def preview(self, objects, num):
        """Preview the obejects pending to be changed
           objects: "clusters" by fingerprint, returned from keyCollisionClustering
           num: the number of objects you want to preview.
        """
        samples = objects.take(num)
        for i,obj in enumerate(samples):
            fingerPrintMapper, info, d, ids = obj
            print("------------ Cluster %d -------------------" % i) 
            for key,count in d.most_common():
                print("Name: %s, Count:%d" %(key,count))
            print("-----------------------------------------------")
            print("Will be changed to \"%s\", takes %d/%d" % info)
    def resolve(self, colName, objects):
        """Resolve the changes
           colName: the column to apply this change
           objects: "clusters" by fingerprint, returned from keyCollisionClustering
        """
        objList = objects.collect()
        def applyToTransformer(x):
            fingerPrintMapper, info, d, ids = x
            str_to_replace = info[0]
            list_str = list(d.keys())
            self._tf.lookup(colName, str_to_replace, list_str)
        for obj in objList:
            applyToTransformer(obj)
        totalRowsAffected = objects.map(lambda x:x[1][2]).reduce(lambda x,y:x+y)
        print("Total rows affected: %d rows" % totalRowsAffected)








