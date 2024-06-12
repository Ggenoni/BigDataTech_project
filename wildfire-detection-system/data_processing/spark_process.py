from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WildfireDataProcessing").getOrCreate()

# Load raw data from Kafka
weather_df = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "weather-data").load()

# Process data (example transformation)
processed_df = weather_df.selectExpr("CAST(value AS STRING) as json").select("json.*")

# Save processed data to MongoDB
processed_df.write.format("mongo").mode("append").option("uri", "mongodb://root:example@mongo:27017/wildfire_detection.processed_data").save()

spark.stop()
