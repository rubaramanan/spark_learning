import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf


@udtf(returnType='hashtag: string')
class hashExtractor:
    def eval(self, text: str):
        if text:
            matches = re.findall(r'#\w+', text)
            for hashtag in matches:
                yield (hashtag,)


session = SparkSession.builder \
    .config('spark.sql.execution.pythonUDTF.enabled', 'true') \
    .appName('udtf') \
    .getOrCreate()

print('-------- using with spark sql ----------')
session.udtf.register('extract_hashtag', hashExtractor)

session.sql("select * from extract_hashtag('Hi #ruba # i love #you.')").show()

data = [("Learning AI with #ruba",), ("Hello# ruba #ramanan",), ("Hi #how are you?",)]

print('-----------using udtf with lateral join-------')

df = session.createDataFrame(data, ['text'])
df.createOrReplaceTempView('tweets')

session.sql("select text, hashtag from tweets, lateral extract_hashtag(text)").show()

session.stop()
