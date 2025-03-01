from pyspark.sql import SparkSession

session = SparkSession.builder.appName('Basic').getOrCreate()

df = session.read.csv('/opt/bitnami/spark/data/fakefriends.csv')

print(df.show())

# DataFrame operations
# Print the schema in a tree format
df.printSchema()

# Select only the "name" column
df.select('_c1').show()

# Select everybody, but increment the age by 1
# df[[df['_c1'], df['_c2']+1]].show()
df.select(df['_c1'], df['_c2'] + 1).show()

# Select people older than 21
df.filter(df['_c2'] > 29).show()

# Count people by age
df.groupBy(df['_c2']).count().orderBy('_c2').show(10)

session.stop()
