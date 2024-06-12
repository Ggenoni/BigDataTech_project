from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("WildfireModelTraining").getOrCreate()

# Load processed data from MongoDB
df = spark.read.format("mongo").option("uri", "mongodb://root:example@mongo:27017/wildfire_detection.processed_data").load()

# Prepare data for training
assembler = VectorAssembler(inputCols=["temperature", "humidity", "wind_speed", "vegetation_index"], outputCol="features")
rf = RandomForestClassifier(labelCol="fire_occurrence", featuresCol="features")
pipeline = Pipeline(stages=[assembler, rf])

# Train model
model = pipeline.fit(df)

# Save the model
model.write().overwrite().save("/app/wildfire_model")

spark.stop()
