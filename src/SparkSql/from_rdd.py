from pyspark.sql import SparkSession, Row

session = SparkSession.builder.appName('RDD').getOrCreate()


def get_data(line: str):
    line = line.split(',')
    return Row(name=line[1], age=int(line[2]))


sc = session.sparkContext
file = sc.textFile('/opt/bitnami/spark/data/fakefriends.csv')
lines = file.map(get_data)

sc_people = session.createDataFrame(lines)
sc_people.createOrReplaceTempView('people')

session.sql('select * from people').show()

session.stop()
