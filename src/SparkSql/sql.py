from pyspark.sql import SparkSession

session = SparkSession.builder.appName('SQL').getOrCreate()

df = session.read.csv('/opt/bitnami/spark/data/fakefriends.csv')

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView('friends')

sqlDf = session.sql('select * from friends where _c2 > 29')
print(sqlDf.show())

# Register 2 global views in different sessions
df.createGlobalTempView('people')
session.sql('select * from global_temp.people').show()

session.newSession().sql('select * from global_temp.people').show()

session.stop()
