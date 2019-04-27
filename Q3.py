from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Q3").setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)

business = sc.textFile("business.csv").map(lambda x:x.split("::"))
review = sc.textFile("review.csv").map(lambda x:x.split("::"))

business_set = business.map(lambda x:(x[0], (x[1], x[2])))

#calculate average based on business_id
sum = review.map(lambda x:(x[2], float(x[3]))).reduceByKey(lambda x, y:x+y)
count = review.map(lambda x:(x[2], 1)).reduceByKey(lambda x, y:x+y)
join_result = sum.join(count)
avg = join_result.map(lambda x:(x[0], x[1][0]/x[1][1]))

#eliminate dulplicate
temp = business_set.join(avg).distinct().collect()

#find top ten rating business_id and then join with business rdd
result = sc.parallelize(temp).top(10, key=lambda x:x[1][1])
final = sc.parallelize(result).map(lambda x:str(x[0])+"\t"+str(x[1][0][0])+"\t"+str(x[1][0][1])+"\t"+str(x[1][1]))

final.saveAsTextFile("Q3")
