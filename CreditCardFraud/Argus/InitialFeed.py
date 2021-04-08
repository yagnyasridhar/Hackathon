from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import math

sc = SparkContext(appName="Initial Feed to Cassandra").getOrCreate()
sql = SQLContext(sc)

customerSchema = StructType().add("cc_num", StringType(), nullable=True).add("first", StringType(), nullable=True).add("last", StringType(), nullable=True).add("gender", StringType(), nullable=True).add("street", StringType(), nullable=True).add("city", StringType(), nullable=True).add("state", StringType(), nullable=True).add("zip", StringType(), nullable=True).add("lat", DoubleType(), nullable=True).add("long", DoubleType(), nullable=True).add("job", StringType(), nullable=True).add("dob", TimestampType(), nullable=True)
transactionSchema = StructType().add("cc_num", StringType(), nullable=True).add("first", StringType(), nullable=True).add("last", StringType(), nullable=True).add("trans_num", StringType(), nullable=True).add("trans_date", StringType(), nullable=True).add("trans_time", StringType(), nullable=True).add("unix_time", LongType(), nullable=True).add("category", StringType(), nullable=True).add("merchant", StringType(), nullable=True).add("amt", DoubleType(), nullable=True).add("merch_lat", DoubleType(), nullable=True).add("merch_long", DoubleType(), nullable=True)

def getDistance(lat1, log1, lat2, log2):
    r = 6371
    latDistance = math.radians(lat2-lat1)
    logDistance = math.radians(log2-log1)
    a = math.sin(latDistance/2) * math.sin(latDistance/2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(logDistance / 2) * math.sin(logDistance / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = r * c
    return distance

if __name__ == "__main__":
    customerDF = sql.read.format("csv").option("header", True).schema(customerSchema).load("Customer.csv")
    customerDF.write.format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "").option("spark.cassandra.connection.port", "").option("keyspace","").option("table","").mode("append").save()
    customerAgeDF = customerDF.withColumn("age", (datediff(current_date(),to_date("dob"))/365).cast(IntegerType))
        
    transactionDF =  sql.read.format("csv").option("header", True).schema(transactionSchema).load("transactions.csv").withColumn("trans_date", split("trans_date", "T").getItem(0)).withColumn("trans_time", concat_ws(" ", "trans_date", "trans_time")).withColumn("trans_time", to_timestamp("trans_time", "YYYY-MM-dd HH:mm:ss"))
    processedDF = transactionDF.join(broadcast(customerAgeDF), Seq("cc_num")).withColumn("distance", lit(round(distanceUdf("lat", "long", "merch_lat", "merch_long"), 2))).select("cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud")
    processedDF.cache()

    fraudDF = processedDF.filter("is_fraud" == 1)
    nonFraudDF = processedDF.filter("is_fraud" == 0)

    fraudDF.write.format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "").option("spark.cassandra.connection.port", "").option("keyspace","").option("table","").mode("append").save()

    nonFraudDF.write.format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "").option("spark.cassandra.connection.port", "").option("keyspace","").option("table","").mode("append").save()