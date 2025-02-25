from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('spark://spark-master:7077').setAppName('Map')
sc = SparkContext(conf=conf)

file = '/opt/bitnami/spark/data/u.data'

tx = sc.textFile(file)

ratings = tx.map(lambda x: x.split()[2])
ratings.collect()
