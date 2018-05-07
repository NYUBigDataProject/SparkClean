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
from collections import defaultdict



class DataFrameDeduplicator:
    """DataFrameDeduplicater is a class to deduplicate in dataFrames
       It will generate "id" to identify every Row, so if a DataFrame already include "id" column,
       remove it before using this class
    """
    def __init__(self, df):
        self._tf = DataFrameTransformer(df)
        self._tf.addPrimaryKey()
    def keyCollisionClustering(self, colName):
        """colName: the column to be clustered"""
        self._tf._assert_cols_in_df(columns_provided=[colName], columns_df=self._tf._df.columns)
        rdd = self._tf._df.select(["id", colName]).rdd.map(list)
        def getFingerPrint(s):
            s = str(s)
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
        objects = clusters.map(previewMapper).filter(lambda x:len(x[2].keys())>1)
        return colName, objects
    def preview(self, colName, objects, num=-1):
        """Preview the obejects pending to be changed
           objects: "clusters" by fingerprint, returned from keyCollisionClustering
           num: the number of objects you want to preview.
        """
        if num > 0:
            samples = objects.take(num)
        else:
            samples = objects.collect()
        for i,obj in enumerate(samples):
            fingerPrintMapper, info, d, ids = obj
            print("------------ Cluster %d -------------------" % i)
            for key,count in d.most_common():
                print("colName: %s| Item: %s, Count:%d" %(colName,key,count))
            print("colName: %s|"%colName," Will be changed to \"%s\", takes %d/%d" % info)
            print("")
    def resolve(self, colName, objects, optimized = False):
        """Resolve the changes
           colName: the column to apply this change
           objects: "clusters" by fingerprint, returned from keyCollisionClustering
        """
        objList = objects.collect()
        if not optimized:
            def applyToTransformer(x):
                fingerPrintMapper, info, d, ids = x
                str_to_replace = info[0]
                list_str = list(d.keys())
                self._tf.lookup(colName, str_to_replace, list_str)
            for i,obj in enumerate(objList):
                if i % 100 == 0:
                    print("Resolving cluster %d/%d" %(i,len(objList)))
                applyToTransformer(obj)
        else:
            def merge(objList):
                str_to_replace = dict()
                for x in objList:
                    fingerPrintMapper, info, d, ids = x
                    change_to = info[0]
                    list_str = list(d.keys())
                    for s in list_str:
                        str_to_replace[change_to] = s
                return str_to_replace
            str_to_replace = merge(objList)
            self._tf.lookup(colName, str_to_replace, None)


        totalRowsAffected = objects.map(lambda x:x[1][2]).reduce(lambda x,y:x+y)
        print("Total rows affected: %d rows" % totalRowsAffected)
    def localitySensitiveHashing(self, colName, blockSize=6, method = "levenshtein", threshold = 0.81):
        """
        colName: the column to be clustered
        blockSize: size of blocking
        method: methods to calculate the similarity
        threshold: controls how similar two items should be thought to be the same
        """
        self._tf._assert_cols_in_df(columns_provided=[colName], columns_df=self._tf._df.columns)
        rdd = self._tf._df.select(["id", colName]).rdd.map(list)
        methodDict = {
            # Edit Based
            "hamming": textdistance.hamming.normalized_similarity,
            "mlipns": textdistance.mlipns.normalized_similarity,
            "levenshtein": textdistance.levenshtein.normalized_similarity,
            # Token based
            "jaccard": textdistance.jaccard.normalized_similarity,
            "overlap": textdistance.overlap.normalized_similarity,
            "cosine": textdistance.cosine.normalized_similarity,
            # Sequence based
            "lcsseq": textdistance.lcsseq.normalized_similarity,
            "lcsstr": textdistance.lcsstr.normalized_similarity,
            # Phonetic based
            "mra": textdistance.mra.normalized_similarity,
        }
        try:
            sim = methodDict[method]
        except:
            print("Waring: %s is not a valid method\n, changes into levenshtein by default.")
            sim = textdistance.levenshtein.normalized_similarity
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
        def LSHflatMapper(x):
            fingerPrint, l = x
            n = len(fingerPrint)
            res = []
            if n > blockSize:
                for i in range(0,n,blockSize):
                    if i+blockSize>n:break
                    res.append((fingerPrint[i:i+blockSize],l))
            else:
                res.append((fingerPrint,l))
            return res
        def previewMapper(l):
            fingerPrintMapper, raws = l
            words = list(map(lambda x:x[1], raws))
            ids = list(map(lambda x:x[0], raws))
            d = Counter(words)
            info = (d.most_common(1)[0][0],d.most_common(1)[0][1],len(raws))
            return (fingerPrintMapper, info, d, ids)
        def thresholdFlatMapper(x):
            fingerPrintMapper, info, d, ids = x
            keys = list(d.keys())
            res = []
            n = len(keys)
            for i in range(n):
                for j in range(i+1,n):
                    if sim(str(keys[i]),str(keys[j]))>threshold:
                        newCounter = Counter()
                        newCounter[keys[i]] = d[keys[i]]
                        newCounter[keys[j]] = d[keys[j]]
                        cand, freq = newCounter.most_common(1)[0]
                        newInfo = (cand, freq, sum(newCounter.values()))
                        res.append((fingerPrintMapper, newInfo, newCounter, ids))
            return res

        clusters = rdd.map(fingerPrintMapper).flatMap(LSHflatMapper).groupByKey().mapValues(list).filter(lambda x:len(x[1])>1)
        objects = clusters.map(previewMapper).flatMap(thresholdFlatMapper).filter(lambda x:len(x[2].keys())>1)

        return colName, objects
    def buildPairs(self,colNames):
        """
        :return a dataframe of pairs for compairing similarity

        Example.

        df: city| country|population
        =>
        res: city|country|population|id|_city|_country|_population|_id
        """
        self._tf._assert_cols_in_df(columns_provided=colNames, columns_df=self._tf._df.columns)
        tf = self._tf
        schema = tf._df.schema
        tf_copy = DataFrameTransformer(tf._df)
        tf_copy.rename_col(self.colNameMapper(schema))
        res = tf._df.join(tf_copy._df, tf._df.id < tf_copy._df._id)
        colNames += ["id"]
        pick = colNames + list(map(lambda x:"_"+x, colNames))
        return res.select(pick)
    def colNameMapper(self, schema):
        """
        :return [(oldColumnName, newColumnName)] from dataframe schema for rename_cols in df_transformer
        """
        return list(map(lambda x:(x.name, "_"+x.name), schema))
    def recordMatching(self, matchColNames, fixColNames):
        """
        matchColNames: colNames used for keyCollision clustering
        fixColNames: colNames we try to fix
        """
        self._tf._assert_cols_in_df(columns_provided=matchColNames, columns_df=self._tf._df.columns)
        self._tf._assert_cols_in_df(columns_provided=fixColNames, columns_df=self._tf._df.columns)
        colNames = list(set(matchColNames + fixColNames))
        colNameIndex = dict(zip(colNames,range(1,1+len(colNames))))
        rdd = self._tf._df.select(["id"]+colNames).rdd.map(list)
        def getFingerPrint(s):
            s = str(s)
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
        def multiFingerPrinterMapper(x):
            _id = x[0]
            multiFingerPrinter = []
            for s in matchColNames:
                index = colNameIndex[s]
                multiFingerPrinter.append(getFingerPrint(x[index]))
            multiFingerPrinter = tuple(multiFingerPrinter)
            return (multiFingerPrinter, x)
        def previewMapper(l):
            multiFingerPrinter, raws = l
            ids = list(map(lambda x:x[0], raws))
            fixs = dict()
            for s in fixColNames:
                index = colNameIndex[s]
                words = list(map(lambda x:x[index], raws))
                d = Counter(words)
                info = (d.most_common(1)[0][0],d.most_common(1)[0][1],len(raws),d)
                if info[1]!=info[2]:
                    fixs[s] = info
            return (multiFingerPrinter, fixs, ids)
        clusters = rdd.map(multiFingerPrinterMapper).groupByKey().mapValues(list).filter(lambda x:len(x[1])>1)
        objects = clusters.map(previewMapper).filter(lambda x:len(x[1].keys())>0)
        return fixColNames,objects
    def previewReords(self, fixColNames, objects, num=-1):
        """Preview the obejects pending to be changed
           fixColNames: the columns to be fixed, actually not needed in this function, but just want to be symetry with one col case
           objects: "clusters" by multi fingerprint, returned from recordMatching
           num: the number of objects you want to preview.
        """
        if num > 0:
            samples = objects.take(num)
        else:
            samples = objects.collect()
        for i in range(len(samples)):
            multiFingerPrinter, fixs, ids = samples[i]
            print("------------ Cluster %d -------------------" % i)
            print("Record id:",ids)
            for col in fixs.keys():
                Item, Count, Total, d = fixs[col]
                for key,count in d.most_common():
                    print("colName: %s| Item: %s, Count:%d" %(col,key,count))
                print("colName: %s| Change: Will be changed to \"%s\", takes %d/%d" % (col, Item, Count, Total))
            print("")
    def resolveRecords(self, fixColNames, objects):
        """
        fixColNames, objects returned by recordMatching
        """
        totalRowsAffected = 0
        samples = objects.collect()
        for i in range(len(samples)):
            if i % 100 == 0:
                print("Resolving cluster %d/%d" %(i,len(samples)))
            multiFingerPrinter, fixs, ids = samples[i]
            totalRowsAffected += len(ids)
            fixs = list(fixs.items())
            for fix in fixs:
                update_col = fix[0]
                new_value = fix[1][0]
                id_list = ids
                id_col = "id"
                self._tf.replace_by_id(new_value, update_col, id_list, id_col)
        print("Total rows affected: %d rows" % totalRowsAffected)

    def show(self, n=10, truncate=True,  withId = False):
        if withId:
            return self._tf._df.show(n, truncate)
        else:
            return self._tf._df.drop("id").show(n, truncate)
    
    def remove_duplicates(self, cols=None):
        """
        :param cols: List of columns to make the comparison, this only  will consider this subset of columns,
        for dropping duplicates. The default behavior will only drop the identical rows.
        :return: Return a new DataFrame with duplicate rows removed
        """
        assert isinstance(cols, list), "Error, cols argument provided must be a list."

        self._tf.remove_duplicates(cols)

        return self
    
    def get_dataframe(self):
        """
        return the dataframe you have cleaned.
        """
        return self._tf._df.drop("id")














