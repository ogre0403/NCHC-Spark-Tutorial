from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("HelloWorld")
sc = SparkContext(conf = conf)
lines = sc.textFile("README.md")
pythonLines = lines.filter(lambda line: "Python" in line)
result = pythonLines.collect()
for line in result:
    print(line)


