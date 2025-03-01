from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

session = SparkSession.builder.appName('RDD').getOrCreate()


def get_data(line: str):
    line = line.split(',')
    return line[1], line[2]


sc = session.sparkContext
file = sc.textFile('/opt/bitnami/spark/data/fakefriends.csv')
lines = file.map(get_data)

schema_string = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split()]
schema = StructType(fields)

sc_people = session.createDataFrame(lines)
sc_people.createOrReplaceTempView('people')

session.sql('select * from people').show(10)

session.stop()
