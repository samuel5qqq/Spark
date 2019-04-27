from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Q4").setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)

business = sc.textFile("business.csv").map(lambda x : x.split("::"))
review = sc.textFile("review.csv").map(lambda x : x.split("::"))

#find business_id in Palo Alto
palo_alto = business.filter(lambda x : "Palo Alto" in x[1]).map(lambda x : (x[0], x[1]))
review_set = review.map(lambda x : (x[2], (x[1], float(x[3]))))

#join with review
temp_join = palo_alto.join(review_set).map(lambda x : x[1][1]).distinct().collect()
temp = sc.parallelize(temp_join)

#find user_id's average rating
sum = temp.reduceByKey(lambda x, y:x+y)
count = temp.map(lambda x:(x[0], 1)).reduceByKey(lambda x, y:x+y)
join_result = sum.join(count)
avg = join_result.map(lambda x:(x[0], x[1][0]/x[1][1]))

result = avg.map(lambda x:str(x[0])+"\t"+str(x[1]))
result.saveAsTextFile("Q4")