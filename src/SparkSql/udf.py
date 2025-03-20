from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def upper_convert(string: str):
    return string.upper()


session = SparkSession.builder.appName('udf').getOrCreate()

print('---------using the spark data frame-------------')
df = session.read.option('header', 'true').csv(r'/opt/bitnami/spark/data/fakefriends-header.csv')

df = df.withColumn('upper name', upper_convert(df['name']))

df.show()

print('-----------using the spark sql-------------')

# we have to register the udf function
session.udf.register('upper_convert', upper_convert)

session.sql("select upper_convert('Hello Ruba') as upper").show()

session.stop()
