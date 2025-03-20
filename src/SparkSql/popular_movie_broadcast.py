import codecs

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StructField, StructType, LongType


def create_lookup():
    file = '/opt/bitnami/spark/data/u.item'
    lookup_table = {}
    with codecs.open(file, 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            lookup_table[int(fields[0])] = fields[1]
    return lookup_table


schema = StructType(
    [
        StructField('user_id', IntegerType(), True),
        StructField('movie_id', IntegerType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', LongType(), True)
    ]
)

session = SparkSession.builder.appName('PopularMovie').getOrCreate()

nameDict = session.sparkContext.broadcast(create_lookup())
rating = session.read.option('sep', '\t').csv('/opt/bitnami/spark/data/u.data',
                                              schema=schema)

rating_count = rating.groupBy('movie_id').count().orderBy(func.desc('count'))


def lookup_name(movie_id):
    return nameDict.value[movie_id]


lookupUdf = func.udf(lookup_name)

rating_count = rating_count.withColumn('movie_name', lookupUdf(func.col('movie_id')))
rating_count.show()

session.stop()
