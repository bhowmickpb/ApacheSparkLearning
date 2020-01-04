import os
import sys



spark_home = os.environ.get('SPARK_HOME','/home/pallab/spark-2.4.2-bin-hadoop2.7')
sys.path.insert(0, spark_home + "/python")

# Add the py4j to the path.
# You may need to change the version number to match your install
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print ("success")

except ImportError as e:
    print ("error importing spark modules", e)
    sys.exit(1)