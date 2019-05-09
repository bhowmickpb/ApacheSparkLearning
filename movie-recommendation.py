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
	from pyspark.sql import SparkSession,Row
	from pyspark.mllib.recommendation import ALS, Rating

	#create SparkContext using standalone mode
	conf= SparkConf().setMaster("local").setAppName("MovieRecommendationSystem")
	sc=SparkContext(conf=conf)
	sc.setCheckpointDir('checkpoint')
	spark = SparkSession.builder.master("local").appName("MovieRecommendationSystem").getOrCreate()
	# create a RDD from text file and transform it to only contains ratings column

	linesRDDMovies = sc.textFile(curr_dir+"/ml-latest-small/movies.csv")
	movieheader = linesRDDMovies.first()
	csvMovies = spark.read.csv(linesRDDMovies, header=True,sep=",").rdd
	
	movies_df =csvMovies.toDF(movieheader.split(','))
	movies_df_panda=movies_df.toPandas()
	movieNamesDict = dict(zip(movies_df_panda.movieId, movies_df_panda.title))

	linesRDDRatings = sc.textFile(curr_dir+"/ml-latest-small/ratings.csv")
	csvRatings = spark.read.csv(linesRDDRatings, header=True,sep=",").rdd
	#ratings_df =csvRatings.toDF(['userId','movieId','rating','timestamp'])
	ratingsRDD=csvRatings.map(lambda x: Rating(int(x['userId']),int(x['movieId']),float(x['rating']))).cache()

	# Build the recommendation model using Alternating Least Squares
	print("Training recommendation model...")
	rank = 10
	numIterations = 6
	model = ALS.train(ratingsRDD, rank, numIterations)

	# the user for which we need to recommend
	#userID = int(sys.argv[1])
	userID = 1

	# lets print the ratings given by this user..
	print("\nRatings given by userID " + str(userID) + ":")
	userRatings = ratingsRDD.filter(lambda l: l[0] == userID)
	for rating in userRatings.collect():
		print (movieNamesDict[str(rating[1])] + ": " + str(rating[2]))
	# now lets use our model to recommend movies for this user..
	print("\nTop 10 recommendations:")
	recommendations = model.recommendProducts(userID, 10)
	for recommendation in recommendations:
		print (movieNamesDict[str(recommendation[1])] + " score " + str(recommendation[2]))

	print('\n')
except ImportError as e:
	print ("error importing spark modules", e)
	sys.exit(1)