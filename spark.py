import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType,StringType
from pyspark.sql.functions import from_json,col
from pyspark.sql.functions import *
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")


def create_spark_session():
    """
    Creates the Spark Session with suitable configs.
    """
    spark = None
    try:
        # Spark session is established with cassandra and kafka jars. Suitable versions can be found in Maven repository.
        spark = SparkSession \
                .builder \
                .appName("SparkStructuredStreaming") \
                .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
                .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
                .config("spark.cassandra.connection.host", "cassandra") \
                .config("spark.cassandra.connection.port","9042")\
                .config("spark.cassandra.auth.username", "cassandra") \
                .config("spark.cassandra.auth.password", "cassandra") \
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session created successfully')
    except Exception:
        logging.error("Couldn't create the spark session")

    return spark
def create_initial_dataframe(spark_session):
    """
    Reads the streaming data and creates the initial dataframe accordingly.
    """
    df = None
    try:
        # Gets the streaming data from topic random_names
        df = spark_session \
              .readStream \
              .format("kafka") \
              .option("kafka.bootstrap.servers", "broker:29092") \
              .option("subscribe", "random_names") \
              .option("delimeter",",") \
              .option("startingOffsets", "earliest") \
              .option("endingOffsets", "latest") \
              .load()
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")

    return df


def create_final_dataframe(df, spark_session):
    """
    Modifies the initial dataframe, and creates the final dataframe.
    """
    if df is None:
        # Handle the case when df is None
        return None

    schema = StructType([
                StructField("id",StringType(),False),
                StructField("first_name ",StringType(),False),
                StructField("last_name ",StringType(),False),
                StructField("gender",StringType(),False),
                StructField("email",StringType(),False),
                StructField("phone",StringType(),False),
                StructField("address",StringType(),False),
                StructField("registered_date ",StringType(),False),
                StructField("picture",StringType(),False)
            ])

    df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),schema).alias("data")).select("data.*")
    print(df)
    return df


def start_streaming(df):
    """
    Starts the streaming to table spark_streaming.random_names in cassandra
    """
    my_query = None
    if df is not None:

        logging.info("Streaming is being started...")
        my_query = (df.writeStream
                      .format("org.apache.spark.sql.cassandra")
                      .outputMode("append")
                      .options(table="random_names", keyspace="spark_streaming")\
                      .start())
        return my_query.awaitTermination()

    else:
        print("Initial dataframe is None. Unable to execute the query.")

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def write_streaming_data():

    spark = create_spark_session()
    df_task = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df_task, spark)
    start_streaming(df_final)


if __name__ == '__main__':
    write_streaming_data()

