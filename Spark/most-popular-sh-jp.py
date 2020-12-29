from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("mostPopularSuperHero").getOrCreate()

schema = StructType([StructField("id", IntegerType(), True), \
                    StructField("name", StringType(), True)])

# df that has id and name of marvel superheroes
nameTabDf = spark.read.option("sep", " ").schema(schema).csv("file:///SparkCourse/Marvel-names.txt")
# df that has marvel superheroes appearances together
graphDf = spark.read.text("file:///SparkCourse/Marvel-graph.txt")

# make connections df where each marvel superhero has appeared with other superheroes,
# it can appear in multiple lines/times in the the data file so need to sum it up
connections = graphDf.withColumn("id", func.split("value"," ")[0]) \
    .withColumn("connection", func.size(func.split("value"," ")) - 1) \
    .groupBy("id").agg(func.sum("connection").alias("totalConnections"))


# ********find most popular superhero************
# to find the most popular sh, sort the connections df and order desc than get the first value
mostPopularSH = connections.sort(func.col("totalConnections").desc()).first()
# join the SH with nameTab to get the name of the SH
mostPopularShName = nameTabDf.filter(func.col("id") == mostPopularSH[0]).select("name").first()
print(str(mostPopularShName[0]) + " is the most popular superhero with " + str(mostPopularSH[1]) + " connections")


# ********list SH with only one connection************
listOneConnection = connections.filter(func.col("totalConnections") == 1)
mostObscure = listOneConnection.join(nameTabDf, "id").select("name")
print("Following Super heroes have only appeared only once with other SH: ")
mostObscure.show(mostObscure.count())

# *********** Minimum Connections *******
minConnection = connections.agg(func.min("totalConnections")).first()[0]
minConnections = connections.filter(func.col("totalConnections") == minConnection)
minConnectionsName = minConnections.join(nameTabDf,"id").select("name")
print("Following Super heroes have min connections with other SH: with min connection as " + str(minConnection))
minConnectionsName.show(minConnectionsName.count())

spark.stop()

