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
	movieId = '1721'   # movieID of Titanic is 1721
	movieIdColIdx = 0 # 1st column has movieId in data file
	ratingColIdx = 2  # 3rd column has the actual rating given by a user
	# create a RDD from text file and transform it to only contains ratings column
	linesRDDMovies = sc.textFile(curr_dir+"/ml-latest-small/movies.csv")
	dfMovies = spark.read.csv(linesRDDMovies, header=True,sep=",").rdd
	moviesRDD = dfMovies.filter(lambda x: x[movieIdColIdx]==movieId)
	# use RDD action countByValue to count how many times each value occurs..
	result = moviesRDD.countByValue()

	linesRDDRatings = sc.textFile(curr_dir+"/ml-latest-small/ratings.csv")
	dfRatings = spark.read.csv(linesRDDRatings, header=True,sep=",").rdd
	ratingsRDDFilter =dfRatings.filter(lambda x: x[1]==movieId)
	ratingsHistogram = ratingsRDDFilter.map(lambda x: (x[ratingColIdx], 1)).reduceByKey(lambda a, b: a + b).collect()
	print('\nRating distribution is for movie Titanic', ratingsHistogram)
	print('\n')
except ImportError as e:
	print ("error importing spark modules", e)
	sys.exit(1)