from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StructType

schema = StructType([
    StructField('user_id', IntegerType(), True),
    StructField('movie_id', IntegerType(), True),
    StructField('ratings', IntegerType(), True)
])

session = SparkSession.builder.appName('item-based col').getOrCreate()

rating = session.read.option('sep', '\t').csv('/opt/bitnami/spark/data/u.data',
                                              schema=schema)
rating.persist(StorageLevel.DISK_ONLY)
r = rating.join(rating, 'user_id')
print(r.collect())
