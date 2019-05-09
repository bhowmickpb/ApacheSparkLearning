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
	from pyspark.sql import Row

	#create SparkContext using standalone mode
	conf= SparkConf().setMaster("local").setAppName("RatingsHistogram")
	sc=SparkContext(conf=conf)
	spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()
	movieId = '1721'   # movieID of Titanic is 1721
	movieIdColIdx = 0 # 1st column has movieId in data file
	ratingColIdx = 2  # 3rd column has the actual rating given by a user
	# create a RDD from text file and transform it to only contains ratings column

	linesRDDRatings = sc.textFile(curr_dir+"/ml-latest-small/ratings.csv")
	csvRatings = spark.read.csv(linesRDDRatings, header=True,sep=",").rdd
	ratings_df =csvRatings.toDF(['userId','movieId','rating','timestamp'])
	print ratings_df.show()

	ratings_df.createOrReplaceTempView("movie_ratings")

	# SQL can be run over DataFrames that have been registered as a table.
	query = "SELECT movieId, count(rating) as cnt FROM movie_ratings GROUP BY movieId order by cnt desc limit 10"
	top_rated = spark.sql(query)

	# The results of SQL queries are RDDs and support all the normal RDD operations.
	print('Most rated movies are:\n')
	for top_movie in top_rated.collect():
	  print(top_movie)
	  
	print('\n')
	spark.stop()
except ImportError as e:
	print ("error importing spark modules", e)
	sys.exit(1)