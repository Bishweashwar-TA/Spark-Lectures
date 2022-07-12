import collections
import re  # regular expression used for text optimizations

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("count")
sc = SparkContext(conf=conf)


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


lines = sc.textFile("file:/home/bishweashwar/tiger_training/spark-lectures/Book")
splitWords = lines.flatMap(normalizeWords)
wordCount = splitWords.countByValue()
sortedOrder = collections.OrderedDict(sorted(wordCount.items()))
for words, count in sortedOrder.items():
    cleanedWord = words.encode('ascii', 'ignore')
    if(cleanedWord):
        print(cleanedWord, count)
