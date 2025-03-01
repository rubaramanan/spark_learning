from pyspark.sql import SparkSession

session = SparkSession.builder.appName('CSV').getOrCreate()

df = session.read.option('header', 'true').option('inferSchema', 'true').csv(
    '/opt/bitnami/spark/data/fakefriends-header.csv')

df.printSchema()

df.select('name').show()

teen = df.filter(df.age > 29)
teen.show()

teen.groupBy('age').count().orderBy('age').show()

teen.select(teen.name, teen.age + 10).show()

session.stop()
