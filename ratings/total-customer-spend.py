from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("totalCustomerSpend")
sc = SparkContext(conf = conf)

def parseLineInRDD(line):
    """ Docstring - this function parse each line by a delimeter
    parameter: Line
    output: fields in the line
    """
    parserLine = line.split(',')
    custID = int(parserLine[0])
    custAmount = float(parserLine[2])
    return (custID, custAmount)

#create a rdd to assign the values in the flat file
lines = sc.textFile("file:///sparkcourse/customer-orders.csv")
#create a map and assign it to new RDD, map takes a funtion or lambda arg
rdd = lines.map(parseLineInRDD)

#since rdd has key, we can agg or reduce it by key directly
totalAmtByCust = rdd.reduceByKey(lambda x, y: (x + y))

# Sorting
# sortedResults = collections.OrderedDict(sorted(totalAmtByCust.items()))
# tuple unpacking is not available in python3 so lambda (x,y):(y,x) is not allowed
sortedResults = totalAmtByCust.map(lambda x: (x[1],x[0])).sortByKey()
sortedResultsDict = sortedResults.collect()

for result in sortedResultsDict:
    print(result)

# Spark Collect all keys and print out the values
# for result in results:
#    print(result)