from pyspark.sql import SparkSession

session = SparkSession.builder.appName('word count').getOrCreate()

words = session.read.text('/opt/bitnami/spark/data/Book')

words.selectExpr("split(value, ' ') as word").show()
