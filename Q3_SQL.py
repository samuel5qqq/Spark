from pyspark.sql import SparkSession
from pyspark.sql.functions import split, mean, desc

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

review = spark.read.text("review.csv")
business = spark.read.text("business.csv")

review_col = split(review['value'], '::')

#create review dataframe
review = review.withColumn('business_id', review_col.getItem(2)).withColumn('star', review_col.getItem(3)).drop('value')
#calculate average based on business_id
review = review.groupBy("business_id").agg(mean("star").alias("avg_star")).orderBy(desc("avg_star")).limit(10)

business_col = split(business['value'], '::')
business = business.withColumn('business_id', business_col.getItem(0)).withColumn('address', business_col.getItem(1)) \
    .withColumn('categories', business_col.getItem(2)).drop('value')

#join review with business
result = business.join(review, business['business_id'] == review['business_id'], 'inner').drop(review.business_id).dropDuplicates().limit(10)

#convert to rdd foramt to output
out = result.rdd.map(list).map(lambda x:str(x[0])+"\t"+str(x[1])+"\t"+str(x[2])+"\t"+str(x[3]))
out.saveAsTextFile("Q3_SQL")
