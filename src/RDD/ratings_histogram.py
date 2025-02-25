from collections import OrderedDict

from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('spark://spark-master:7077').setAppName('Rating Histogram')
sc = SparkContext(conf=conf)

file = sc.textFile('/opt/bitnami/spark/data/u.data')

ratings = file.map(lambda s: s.split()[2])
count = ratings.countByValue()

or_dic = OrderedDict(sorted(count.items(), reverse=True))
for k, v in or_dic.items():
    print(k, v)
