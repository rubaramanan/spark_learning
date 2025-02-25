from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('spark://spark-master:7077').setAppName('minTemp')
sc = SparkContext(conf=conf)

file = sc.textFile("/opt/bitnami/spark/data/1800.csv")

# celcious
lines = file.map(lambda x: x.split(','))
f_min = lines.filter(lambda x: 'TMAX' in x[2])
f_mins = f_min.map(lambda x: (x[0], float(x[3]) * 0.1))
min_temps = f_mins.reduceByKey(lambda x, y: max(x, y))
for i in min_temps.collect():
    print(f"Station: {i[0]}, Temp: {i[1]:.2f}C")


# farahenite
def parselein(line: str):
    line = line.split(',')
    station_id = line[0]
    entity_type = line[2]
    temperature = float(line[3]) * 0.1 * (9 / 5) + 32
    return (station_id, entity_type, temperature)


lines = file.map(parselein)
t_mins = lines.filter(lambda x: 'TMAX' in x[1])
t_min = t_mins.map(lambda x: (x[0], x[2]))
min_temps = t_min.reduceByKey(lambda x, y: max(x, y))
for i in min_temps.collect():
    print(f"Station: {i[0]}, Temp: {i[1]:.2f}F")
