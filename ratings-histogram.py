import sys,csv,os
import collections


curr_dir = os.getcwd()
spark_home = os.environ.get('SPARK_HOME','/home/pallab/spark-2.4.2-bin-hadoop2.7')
sys.path.insert(0, spark_home + "/python")

# Add the py4j to the path.
# You may need to change the version number to match your install
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))

try:
	from pyspark import SparkContext,SparkConf
	from pyspark.sql import SparkSession

	#create SparkContext using standalone mode
	conf= SparkConf().setMaster("local").setAppName("RatingsHistogram")
	sc=SparkContext(conf=conf)
	spark = SparkSession.builder.master("local").appName("RatingsHistogram").config("spark.csv.config.option", "ratings").getOrCreate()
	# create a RDD from text file and transform it to only contains ratings column
	linesRDD = sc.textFile(curr_dir+"/ml-latest-small/ratings.csv")
	df = spark.read.csv(linesRDD, header=True,sep=",").rdd
	print linesRDD.count()
	ratingsRDD = df.map(lambda x: x[2])
	# use RDD action countByValue to count how many times each value occurs..
	result = ratingsRDD.countByValue()
	# print out the results
	sortedResults = collections.OrderedDict(sorted(result.items()))
	print('\nRating Count')
	for key, value in sortedResults.items():
		print("%s %i" % (key, value))
except ImportError as e:
	print ("error importing spark modules", e)
	sys.exit(1)