from pyspark.sql import SparkSession
from pyspark.sql.functions import split, mean, desc

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

review = spark.read.text("review.csv")
business = spark.read.text("business.csv")

#create business dataframe
business_col = split(business['value'], '::')
business = business.withColumn('business_id', business_col.getItem(0)).withColumn('address', business_col.getItem(1)).drop('value')
business = business.filter(business['address'].like('%Palo Alto%')).drop('address')

#create review dataframe
review_col = split(review['value'], '::')
review = review.withColumn('user_id', review_col.getItem(1)).withColumn('business_id', review_col.getItem(2))\
    .withColumn('star', review_col.getItem(3)).drop('value')

#join two dataframe on same business_id and then calculate user_id's average rating
join_result = business.join(review, business['business_id'] == review['business_id'], 'inner').drop('business_id').dropDuplicates()
result = join_result.groupBy("user_id").agg(mean("star").alias("avg_star"))

#convert to rdd foramt to output
out = result.rdd.map(list).map(lambda x:str(x[0])+"\t"+str(x[1]))
out.coalesce(1, True).saveAsTextFile("Q4_SQL")