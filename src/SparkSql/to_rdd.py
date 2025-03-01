from pyspark.sql import SparkSession

session = SparkSession.builder.appName('rdd').getOrCreate()
df = session.read.csv('/opt/bitnami/spark/data/fakefriends.csv')

lines = df.rdd.map(lambda x: x._c1)

for line in lines.collect():
    print(line)

session.stop()
