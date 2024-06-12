FROM bitnami/spark:3.1.2

WORKDIR /app

COPY . /app

CMD ["spark-submit", "--master", "local[*]", "spark_train.py"]
