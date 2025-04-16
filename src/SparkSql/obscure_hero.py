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

graphWithHero = graph.withColumn('split', func.split(func.trim(func.col('value')), ' ')) \
    .withColumn('hero_id', func.col('split')[0]) \
    .withColumn('connections', func.size(func.col('split')) - 1) \
    .groupBy('hero_id').agg(func.sum(func.col('connections')).alias('total_connections'))

minConnCount = graphWithHero.agg(func.min('total_connections')).first()[0]

sortHeros = graphWithHero.filter(func.col('total_connections') == minConnCount)


# obscure_hero = names.join(sortHeros, names.hero_id == sortHeros.hero_id).select(names.hero_name, sortHeros.total_connections)
obscure_hero = sortHeros.join(names, 'hero_id').select(names.hero_name, sortHeros.total_connections)

for hero in obscure_hero.collect():
    print(f"Obscure hero is {hero['hero_name']}, with {hero['total_connections']}")

session.stop()
