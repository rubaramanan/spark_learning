from pyspark.sql import SparkSession
from pyspark.sql import functions as func

session = SparkSession.builder.appName('friends').getOrCreate()

people = session.read.option('header', 'true').option('inferSchema', 'true').csv(
    '/opt/bitnami/spark/data/fakefriends-header.csv')

people = people.select('age', 'friends')

people.groupBy('age').avg('friends').orderBy('age').show()

people.groupBy('age').avg('friends').sort('age').show()

# format with avg friend to 2 decimal
people.groupBy('age').agg(func.round(func.avg(people.friends), 2)).sort('age').show()

# give column name for avg column
people.groupBy('age').agg(func.round(func.avg(people.friends), 2).alias('friends_avg')).sort('age').show()

session.stop()
