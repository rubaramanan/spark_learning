from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('spark://spark-master:7077').setAppName('Total By Customer')
sc = SparkContext(conf=conf)

file = sc.textFile('/opt/bitnami/spark/data/customer-orders.csv')

def get_data(line: str):
    line = line.split(',')
    client_id = int(line[0])
    sales = float(line[2])
    return (client_id, sales)


line = file.map(get_data)
total_sales = line.reduceByKey(lambda x, y: x+y)

sort = total_sales.map(lambda xy: (xy[1], xy[0])).sortByKey()
for i in sort.collect():
    print(i[1], i[0])