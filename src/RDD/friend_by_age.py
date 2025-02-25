from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('spark://spark-master:7077').setAppName('FriendsRdd')
sc = SparkContext(conf=conf)

file = sc.textFile('/opt/bitnami/spark/data/fakefriends.csv')


def get_age_friends(line):
    line = line.split(',')
    age = int(line[2])
    num_frd = int(line[3])
    return age, num_frd


# get age, num_friends and number of entries by age

step_o = file.map(get_age_friends)
step_tw = step_o.mapValues(lambda x: (x, 1))

totalFriendByAge = step_tw.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

averageAge = totalFriendByAge.mapValues(lambda x: x[0] / x[1])

for i in averageAge.collect():
    print(i)
