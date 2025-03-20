from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StructField, StructType, LongType, StringType

schema = StructType(
    [
        StructField('user_id', IntegerType(), True),
        StructField('movie_id', IntegerType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', LongType(), True)
    ]
)

session = SparkSession.builder.appName('PopularMovie').getOrCreate()

rating = session.read.option('sep', '\t').csv('/opt/bitnami/spark/data/u.data',
                                              schema=schema)

rating_count = rating.groupBy('movie_id').count().orderBy(func.desc('count'))
rating_count.show()

movie_schema = StructType([
    StructField('movie_id', IntegerType(), True),
    StructField('movie_name', StringType(), True)
])
movie = session.read.option('sep', '|').csv('/opt/bitnami/spark/data/u.item',
                                            schema=movie_schema)

final = rating_count.join(movie, rating_count.movie_id == movie.movie_id) \
    .select(movie['movie_name'], rating_count['count']).orderBy(func.desc('count'))
final.show()
session.stop()
