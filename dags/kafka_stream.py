import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}


def create_cassandra_connection():
    from cassandra.cluster import Cluster

    try:
        cluster = Cluster(['cassandra'], port=9042)
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None
    
def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS kafka_stream
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace created successfully!")
    except Exception as e:
        logging.error(f"Error creating keyspace: {e}")

def create_tracking_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS kafka_stream.tracking
            (
                create_time  timeuuid PRIMARY KEY,
                bid          double,
                bn           text,
                campaign_id  double,
                cd           double,
                custom_track text,
                de           text,
                dl           text,
                dt           text,
                ed           text,
                ev           double,
                group_id     double,
                id           text,
                job_id       double,
                md           text,
                publisher_id double,
                rl           text,
                sr           text,
                ts           text,
                tz           double,
                ua           text,
                uid          text,
                utm_campaign text,
                utm_content  text,
                utm_medium   text,
                utm_source   text,
                utm_term     text,
                v            double,
                vp           text
            );
        """)
        logging.info("Tracking table created successfully!")
    except Exception as e:
        logging.error(f"Error creating tracking table: {e}")

def import_table_to_cassandra():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

    session = create_cassandra_connection()
    if not session:
        return
    
    create_keyspace(session)
    create_tracking_table(session)

    try:
        spark = SparkSession.builder.config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.3.0') \
                                    .config('spark.cassandra.connection.host', 'cassandra') \
                            .getOrCreate()

        schema = StructType([
                StructField("number", IntegerType(), True),
                StructField("create_time", StringType(), True),
                StructField("bid", DoubleType(), True),
                StructField("bn", StringType(), True),
                StructField("campaign_id", DoubleType(), True),
                StructField("cd", DoubleType(), True),
                StructField("custom_track", StringType(), True),
                StructField("de", StringType(), True),
                StructField("dl", StringType(), True),
                StructField("dt", StringType(), True),
                StructField("ed", StringType(), True),
                StructField("ev", DoubleType(), True),
                StructField("group_id", DoubleType(), True),
                StructField("id", StringType(), True),
                StructField("job_id", DoubleType(), True),
                StructField("md", StringType(), True),
                StructField("publisher_id", DoubleType(), True),
                StructField("rl", StringType(), True),
                StructField("sr", StringType(), True),
                StructField("ts", StringType(), True),
                StructField("tz", DoubleType(), True),
                StructField("ua", StringType(), True),
                StructField("uid", StringType(), True),
                StructField("utm_campaign", StringType(), True),
                StructField("utm_content", StringType(), True),
                StructField("utm_medium", StringType(), True),
                StructField("utm_source", StringType(), True),
                StructField("utm_term", StringType(), True),
                StructField("v", DoubleType(), True),
                StructField("vp", StringType(), True),
            ])

        df = spark.read.csv("/usr/local/airflow/include/dataset/tracking.csv", header=True, schema=schema)
        df = df.drop(df.columns[0])

        df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="tracking", keyspace="kafka_stream") \
          .mode("append") \
          .save()

        logging.info("Data imported to Cassandra successfully!")
    except Exception as e:
        logging.error(f"Error importing data to Cassandra: {e}")
    finally:
        spark.stop()

def fetch_data_from_cassandra():
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
    
    cluster = Cluster(['cassandra'])
    
    try:
        session = cluster.connect('kafka_stream')
        query = SimpleStatement("SELECT * FROM tracking", fetch_size=10)
        result = session.execute(query)
        return result
    except Exception as e:
        logging.error(f'Error executing Cassandra query: {e}')
        return None
    finally:
        cluster.shutdown()

def uuid_to_timestamp(uuid_string):
    import uuid
    import time

    uuid_obj = uuid.UUID(uuid_string)
    timestamp = (uuid_obj.time - 0x01b21dd213814000) / 1e7
    timestamp_datetime = time.gmtime(timestamp)

    return time.strftime("%Y-%m-%d %H:%M:%S", timestamp_datetime)

def format_data(row):
    ts = uuid_to_timestamp(str(row[0]))
    data = {
        'ts': ts,
        'job_id': row[13],
        'custom_track': row[5],
        'bid': row[1],
        'campaign_id': row[3],
        'group_id': row[11],
        'publisher_id': row[15]
    }
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    
    try:
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        curr_time = time.time()

        while True:
            if time.time() > curr_time + 60:
                break
            try:
                rows = fetch_data_from_cassandra()
                for row in rows:
                    row_data = format_data(row)
                    producer.send('users_created', json.dumps(row_data).encode('utf-8'))
                producer.flush()

            except Exception as e:
                logging.error(f'An error occurred: {e}')
                continue
    except Exception as e:
        logging.error(f'Error initializing Kafka producer: {e}')

with DAG('stream_data_tracking',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    import_table_task = PythonOperator(
        task_id = 'import_table_to_cassandra',
        python_callable=import_table_to_cassandra
    )

    write_data = PythonOperator(
        task_id='write_data_to_kafka',
        python_callable=stream_data
    )

    import_table_task >> write_data