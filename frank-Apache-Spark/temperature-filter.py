import collections

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TempMinimum")
sc = SparkContext(conf=conf)


def temp(temperature):
    data = temperature.split(',')
    stationID = data[0]
    Temperature = int(data[3])
    entryType = data[2]
    return (stationID, entryType, Temperature)


lines = sc.textFile("file:/home/bishweashwar/tiger_training/spark-lectures/1800.csv")
rdd = lines.map(temp)

minTemps = rdd.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()

for result in results:
    print(result)
