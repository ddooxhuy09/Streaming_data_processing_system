import findspark
findspark.init()

import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, to_timestamp, lit, current_timestamp, unix_timestamp, expr, window

import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, inspect

import time


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .getOrCreate()

        s_conn.conf.set("spark.sql.streaming.failOnDataLoss", "false")

        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_mysql():
    try:
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="mysql",
            database="spark_stream"
        )
        return db
    except mysql.connector.Error as err:
        print(f"Lỗi MySQL: {err}")
        return None
    
def create_table(connection):
    cursor = connection.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS result (
    job_id                   double           not null,
    ts                       timestamp        null,
    dates                    date             null,
    hours                    int              null,
    publisher_id             double           null,
    campaign_id              double           null,
    group_id                 double           null,
    bid_set                  double           null,
    clicks                   varchar(255)     null,
    spend_hour               double           null,
    conversion               varchar(255)     null,
    qualified_application    varchar(255)     null,
    disqualified_application varchar(255)     null,
    company_id               int              null,
    sources                  varchar(255)     not null
    );
    """

    cursor.execute(create_table_query)
    print("Table 'result' created successfully")

    connection.commit()
    cursor.close()

def import_table_job():
    csv_file_path = 'include/dataset/job.csv'
    df = pd.read_csv(csv_file_path)

    engine = create_engine("mysql+mysqlconnector://{user}:{pw}@localhost:{port}/{db}"
                       .format(user="root", pw="mysql",
                               port="3306", db="spark_stream"))

    table_name = 'job'

    inspector = inspect(engine)

    if table_name in inspector.get_table_names():
        print(f'Bảng {table_name} đã tồn tại trong cơ sở dữ liệu. Không cần nhập lại.')
    else:
        df.to_sql(name=table_name, con=engine, if_exists='fail', index=False)
        print(f'Bảng {table_name} được nhập thành công.')

def calculating_clicks(df,spark):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data = clicks_data.na.fill({'bid':0})
    clicks_data = clicks_data.na.fill({'job_id':0})
    clicks_data = clicks_data.na.fill({'publisher_id':0})
    clicks_data = clicks_data.na.fill({'group_id':0})
    clicks_data = clicks_data.na.fill({'campaign_id':0})
    clicks_data.createOrReplaceTempView('clicks')
    clicks_output = spark.sql("""select job_id , ts, date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , ROUND(avg(bid),2) as bid_set, count(*) as clicks , sum(bid) as spend_hour from clicks
    group by job_id , ts, date(ts) , hour(ts) , publisher_id , campaign_id , group_id""")
    return clicks_output

def calculating_conversion(df, spark):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data.createOrReplaceTempView('conversion')
    conversion_output = spark.sql("""select job_id , ts, date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as conversions from conversion
    group by job_id , ts, date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return conversion_output 

def calculating_qualified(df, spark):    
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.createOrReplaceTempView('qualified')
    qualified_output = spark.sql("""select job_id , ts, date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as qualified from qualified
    group by job_id , ts, date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return qualified_output

def calculating_unqualified(df, spark): 
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.createOrReplaceTempView('unqualified')
    unqualified_output = spark.sql("""select job_id , ts, date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as unqualified from unqualified
    group by job_id , ts, date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return unqualified_output

def process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output):
    final_data = clicks_output.join(conversion_output,['job_id', 'ts', 'date','hour','publisher_id','campaign_id','group_id'],'full').\
    join(qualified_output,['job_id', 'ts','date','hour','publisher_id','campaign_id','group_id'],'full').\
    join(unqualified_output,['job_id', 'ts','date','hour','publisher_id','campaign_id','group_id'],'full')
    return final_data

def process_kafka_data(df, spark):
    clicks_output = calculating_clicks(df, spark)
    conversion_output = calculating_conversion(df, spark)
    qualified_output = calculating_qualified(df, spark)
    unqualified_output = calculating_unqualified(df, spark)
    final_data = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output)
    return final_data

def retrieve_company_data(spark, url, driver, dbtable_company, user, password):
    
    company = spark.read.format("jdbc")\
                .option("url",url)\
                .option("driver",driver)\
                .option("dbtable",dbtable_company)\
                .option("user",user)\
                .option("password",password)\
                .load()
    
    company = company.select("id", "company_id").withColumnRenamed("id", "job_id")
    
    return company

def import_to_mysql(output, url, driver, table, user, password):
    final_output = output.select('job_id', to_timestamp('ts').alias('ts'), 'date', 'hour', 'publisher_id', 'company_id', 'campaign_id','group_id', 
                                 'unqualified', 'qualified', 'conversions', 'clicks', 'bid_set', 'spend_hour')
    final_output = final_output.withColumnRenamed('date', 'dates')\
                               .withColumnRenamed('hour', 'hours')\
                               .withColumnRenamed('qualified', 'qualified_application')\
                               .withColumnRenamed('unqualified', 'disqualified_application')\
                               .withColumnRenamed('conversions', 'conversion')
    
    final_output = final_output.select('job_id', 'ts', 'dates', 'hours', 'publisher_id', 'company_id', 'campaign_id','group_id', 'bid_set', 'clicks', 
                                 'spend_hour', 'conversion', 'qualified_application', 'disqualified_application')
    
    final_output = final_output.withColumn('sources', lit('Cassandra'))

    def foreach_batch_function(df, epoch_id):
        df.write.jdbc(url=url, table=table, mode="append", properties={
            "user": user,
            "password": password,
            "driver": driver
        })
  
    query = final_output\
            .writeStream\
            .outputMode("append")\
            .foreachBatch(foreach_batch_function)\
            .option("checkpointLocation", "C:\\Users\\ddoox\\Documents\\Project_DataEngineer_Streaming_Processing\\tmp\\checkpoint")\
            .trigger(processingTime="500 seconds")\
            .start()

    query.awaitTermination()

    return print('Data imported successfully')

def etl_process(spark, df, url, driver, user, password):
    print('Selecting data from Kafka')
    print('-----------------------------')
    df = df.filter(df.job_id.isNotNull())
    df.printSchema()
    print('-----------------------------')
    print('Processing Kafka Output')
    print('-----------------------------')
    data = process_kafka_data(df, spark)
    dbtable_company = "job"
    company = retrieve_company_data(spark, url, driver, dbtable_company, user, password)
    final_output = data.join(company, 'job_id', 'left')
    return final_output

def read_data_in_kafka(spark):
    print("Read data from Kafka")
    print('-----------------------------')
    spark_df = None
    try:
        spark_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option("failOnDataLoss", "false") \
            .load()
            
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")


    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
                StructField("ts", StringType(), True),
                StructField("job_id", DoubleType(), True),
                StructField("custom_track", StringType(), True),
                StructField("bid", DoubleType(), True),
                StructField("campaign_id", DoubleType(), True),
                StructField("group_id", DoubleType(), True),    
                StructField("publisher_id", DoubleType(), True)
            ])

    df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*").withColumn("ts", to_timestamp(col("ts")))\
        .withWatermark("ts","3 minutes")

    return df

def main_task():

    spark = create_spark_connection()
    connection = connect_to_mysql()

    create_table(connection)
    import_table_job()

    host = 'localhost'
    port = '3306'
    db_name = 'spark_stream'   
    user = 'root'
    password = 'mysql'
    url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
    driver = "com.mysql.cj.jdbc.Driver"
    table = 'result'
    
    print('The host is ', host)
    print('The port using is ', port)
    print('The db using is ', db_name)

    print('Read data in kafka')
    data = read_data_in_kafka(spark)

    print('Format data in kafka')
    data = create_selection_df_from_kafka(data)

    print('ETL data')
    data = etl_process(spark, data, url, driver, user, password)

    print('Import to mysql')
    import_to_mysql(data, url, driver, table, user, password)
    
    print('-----------------------------')
    return print('Task Finished')

main_task()