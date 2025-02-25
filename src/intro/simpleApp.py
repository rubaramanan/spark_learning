from pyspark.sql import SparkSession

log_file = "/opt/bitnami/spark/README.md"

session = SparkSession.builder.appName('SimpleApp').getOrCreate()

log = session.read.text(log_file)

a = log.filter(log.value.contains('a')).count()
b = log.filter(log.value.contains('b')).count()

print(f"numAs = {a}, numBs = {b}")

session.stop()
