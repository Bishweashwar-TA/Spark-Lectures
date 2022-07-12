from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("amount")
sc = SparkContext(conf=conf)

def customer(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

lines = sc.textFile("file:/home/bishweashwar/tiger_training/spark-lectures/customer-orders.csv")
rdd = lines.map(customer)
count = rdd.reduceByKey(lambda x, y: x + y)

countRev = count.map(lambda x: (x[1], x[0]))
flipped = countRev.sortByKey()
countTotal = flipped.collect()

for x in countTotal:
    print(x)
