from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("count")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:/home/bishweashwar/tiger_training/spark-lectures/Book")
splitWords = lines.flatMap(lambda x: x.split())
wordCount = splitWords.countByValue()

for words, count in wordCount.items():
    cleanedWord = words.encode('ascii', 'ignore')
    if(cleanedWord):
        print(cleanedWord, count)
