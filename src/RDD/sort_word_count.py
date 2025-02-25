import re

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('spark://spark-master:7077').setAppName('WordCount')
sc = SparkContext(conf=conf)

file = sc.textFile('/opt/bitnami/spark/data/Book')


def data_cleaning(text: str):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


line = file.flatMap(data_cleaning)
line_c = line.map(lambda x: (x, 1))
word_c = line_c.reduceByKey(lambda x, y: x + y)
# word_c = line.countByValue() # returns default dict
sorted_word = word_c.map(lambda x: (x[1], x[0])).sortByKey()

# for w, c in word_c.items():
#     if clean_word:=w.encode('ascii', 'ignore'):
#         print(clean_word, c)

for i in sorted_word.collect():
    if clean_word := i[1].encode('ascii', 'ignore'):
        print(clean_word, i[0])
