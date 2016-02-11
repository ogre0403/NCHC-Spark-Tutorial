from pyspark import SparkConf, SparkContext

# TODO: Exercise i-4:
# Launch standalone project
conf = SparkConf().setAppName("HelloWorld")
sc = SparkContext(conf = conf)
lines = sc.textFile("README.md")
pythonLines = lines.filter(lambda line: "Python" in line)
result = pythonLines.collect()
for line in result:
    print(line)


