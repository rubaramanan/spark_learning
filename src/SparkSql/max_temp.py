from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import functions as func

session = SparkSession.builder.appName('min temp').getOrCreate()


schema = StructType([
    StructField('station', StringType(), True),
    StructField('date', IntegerType(), True),
    StructField('cat', StringType(), True),
    StructField('temp', FloatType(), True)
])
data = session.read.csv('/opt/bitnami/spark/data/1800.csv',
                        header=False,
                        schema=schema)
# to filter out specific values we can use filter and where equally

min_cat = data.where(data.cat=='TMAX')

min_cat.select('station', 'temp')
tmin = min_cat.groupby('station').min('temp')
tmin.show()

# ftmin = tmin.withColumn('temperature', 
                #  data.temp * 0.1 * (9/5) + 32)

ftmin = tmin.withColumn('temperature',
                        func.round(func.col('min(temp)') * 0.1 * (9/5) + 32, 2)).select('station', 'temperature').sort('temperature')

results = ftmin.collect()

for i in results:
    print(f"Station: {i[0]}, minimum temperature: {i[1]:.2f}")

session.stop()