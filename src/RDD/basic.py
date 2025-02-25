from pyspark import SparkContext, SparkConf

log_file = "/opt/bitnami/spark/README.md"

conf = SparkConf().setMaster('spark://spark-master:7077').setAppName('Basics')
sc = SparkContext(conf=conf)

lines = sc.textFile(log_file)
line_length = lines.map(lambda s: len(s))
total_length = line_length.reduce(lambda a, b: a + b)
print(total_length)
