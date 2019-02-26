
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

my_lines = sc.textFile("file:///Users/sk/Documents/Udemy_sparks/ml-100k/u.data")
print(my_lines.take(5))
ratings=my_lines.flatMap(lambda x: x.split()[2])
#ratings.take(2)

res=ratings.countByValue()
print(res)
my_sortedres=collections.OrderedDict(sorted(res.items()))
#my_sortedre
for key,value in my_sortedres.items():
	print("%s %i" %(key,value))
