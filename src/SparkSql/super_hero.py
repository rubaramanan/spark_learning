from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

session = SparkSession.builder.appName('super hero').getOrCreate()

nameSchema = StructType([
    StructField('hero_id', IntegerType(), True),
    StructField('hero_name', StringType(), True)
])

graph = session.read.text('/opt/bitnami/spark/data/Marvel+Graph')
names = session.read.csv('/opt/bitnami/spark/data/Marvel+Names', sep=' ', schema=nameSchema)

graphWithHero = graph.withColumn('split', func.split(func.col('value'), ' ')) \
    .withColumn('hero_id', func.col('split')[0]) \
    .withColumn('connections', func.size(func.col('split')) - 1) \
    .groupBy('hero_id').agg(func.sum(func.col('connections')).alias('total_connections'))

sortHeros = graphWithHero.sort(func.col('total_connections').desc()).first()

super_hero = names.filter(sortHeros[0] == func.col('hero_id')).select('hero_name').first()
print(super_hero)
print(f"Super hero is {super_hero[0]}, {sortHeros[2]} people liked him.")

session.stop()
