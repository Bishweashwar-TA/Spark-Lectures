import collections

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Friends")
sc = SparkContext(conf=conf)


def parseLine(line):
    data = line.split(',')
    age = int(data[2])
    numFriends = int(data[3])
    return (age, numFriends)


lines = sc.textFile("file:/home/bishweashwar/tiger_training/spark-lectures/fakefriends.csv")
rdd = lines.map(parseLine)

totalByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageAge = totalByAge.mapValues(lambda x: x[0]/x[1])
results = averageAge.collect()
sortedResults = collections.OrderedDict(sorted(results))
for key, value in sortedResults.items():
    print("(%s, %i)" % (key, value))
