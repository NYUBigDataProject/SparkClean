from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext

def loadDemo():
    schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("population", IntegerType(), True)])
    countries = ['Colombia', 'America', 'Brazil', 'Spain', 'America', 'Amarica','Colombia']
    cities = ['Bogota', 'New York', '   São Paulo   ', '~Madrid', 'New York', 'New York  ','Bogotá']
    population = [37800000,9795791,12341418,6489162,19795791,19795791,37800000]
    # Dataframe:
    df = spark.createDataFrame(list(zip(cities, countries, population)), schema=schema)
    return df

