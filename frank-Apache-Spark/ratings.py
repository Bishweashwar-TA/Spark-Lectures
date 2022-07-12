import collections

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Ratings-movies")
sc = SparkContext(conf=conf)

lines = sc.textFile("ml-100k/u.data")

ratings = lines.map(lambda x: x.split()[2])

result = ratings.countByValue()
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
