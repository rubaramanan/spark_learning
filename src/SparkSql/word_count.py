from pyspark.sql import SparkSession
from pyspark.sql import functions as func

session = SparkSession.builder.appName('word count').getOrCreate()

df = session.read.text('/opt/bitnami/spark/data/Book')

# words.selectExpr("split(value, ' ') as word").show()

# explode function exact like flatmap
words = df.select(func.explode(func.split(df.value, '\\W+')).alias('word'))
wordsWithoutEmptyString = words.filter(words.word != "")

wordsLower = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias('word'))
count = wordsLower.groupBy('word').count()
countSort = count.sort('count')
countSort.show()

# it will show all data
countSort.show(countSort.count())
# wordsLower.show()

session.stop()
