import requests
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime
import time

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'random_user_data'

# PostgreSQL Configuration
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5432'
POSTGRES_DB = 'random_user_db'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'password'
POSTGRES_TABLE = 'users'

# Random User Generator API Configuration
RANDOM_USER_API = 'https://randomuser.me/api/'

# PostgreSQL connection and table creation
def create_postgres_table():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            id SERIAL PRIMARY KEY,
            gender VARCHAR(10),
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            street_number INTEGER,
            street_name VARCHAR(100),
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100),
            postcode VARCHAR(20),
            latitude FLOAT,
            longitude FLOAT,
            timezone_offset VARCHAR(10),
            timezone_description VARCHAR(100),
            email VARCHAR(100),
            username VARCHAR(100),
            password VARCHAR(100),
            salt VARCHAR(100),
            md5 VARCHAR(100),
            sha1 VARCHAR(100),
            sha256 VARCHAR(100),
            date_of_birth TIMESTAMP,
            registered_date TIMESTAMP,
            phone VARCHAR(20),
            cell VARCHAR(20),
            ssn VARCHAR(20),
            picture_large VARCHAR(200),
            picture_medium VARCHAR(200),
            picture_thumbnail VARCHAR(200),
            nationality VARCHAR(10)
        )
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Fetch data from Random User Generator API and send to Kafka
def fetch_data_and_send_to_kafka():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    while True:
        response = requests.get(RANDOM_USER_API)
        data = response.json()['results'][0]

        producer.send(KAFKA_TOPIC, value=data)
        producer.flush()

        time.sleep(10)  # Fetch data every 10 seconds

# Consume Kafka messages, update PostgreSQL table
def consume_kafka_and_update_postgres():
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for message in consumer:
        user_data = message.value
        insert_query = f'''
            INSERT INTO {POSTGRES_TABLE} (
                gender, first_name, last_name, street_number, street_name,
                city, state, country, postcode, latitude, longitude,
                timezone_offset, timezone_description, email, username, password,
                salt, md5, sha1, sha256, date_of_birth, registered_date,
                phone, cell, ssn, picture_large, picture_medium, picture_thumbnail,
                nationality
            )
            VALUES (
                '{user_data["gender"]}', '{user_data["name"]["first"]}', '{user_data["name"]["last"]}',
                {user_data["location"]["street"]["number"]}, '{user_data["location"]["street"]["name"]}',
                '{user_data["location"]["city"]}', '{user_data["location"]["state"]}', '{user_data["location"]["country"]}',
                '{user_data["location"]["postcode"]}', {user_data["location"]["coordinates"]["latitude"]},
                {user_data["location"]["coordinates"]["longitude"]}, '{user_data["location"]["timezone"]["offset"]}',
                '{user_data["location"]["timezone"]["description"]}', '{user_data["email"]}', '{user_data["login"]["username"]}',
                '{user_data["login"]["password"]}', '{user_data["login"]["salt"]}', '{user_data["login"]["md5"]}',
                '{user_data["login"]["sha1"]}', '{user_data["login"]["sha256"]}',
                '{user_data["dob"]["date"]}', '{user_data["registered"]["date"]}',
                '{user_data["phone"]}', '{user_data["cell"]}', '{user_data["id"]["value"]}',
                '{user_data["picture"]["large"]}', '{user_data["picture"]["medium"]}', '{user_data["picture"]["thumbnail"]}',
                '{user_data["nat"]}'
            )
#Step 1: Kafka Setup

from kafka import KafkaProducer, KafkaConsumer

#Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'random_user_data'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

# Create Kafka consumer
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
```

Step 2: PostgreSQL Setup and Table Creation
```python
import psycopg2

# PostgreSQL Configuration
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5432'
POSTGRES_DB = 'random_user_db'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'password'
POSTGRES_TABLE = 'users'

# PostgreSQL connection and table creation
def create_postgres_table():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            -- Define your table columns here
        )
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
#Step 3: Fetch data from Random User Generator API and send to Kafka
```python
import requests
import json
import time

# Random User Generator API Configuration
RANDOM_USER_API = 'https://randomuser.me/api/'

# Fetch data from Random User Generator API and send to Kafka
def fetch_data_and_send_to_kafka():
    while True:
        response = requests.get(RANDOM_USER_API)
        data = response.json()['results'][0]

        producer.send(KAFKA_TOPIC, value=json.dumps(data).encode('utf-8'))
        producer.flush()

        time.sleep(10)  # Fetch data every 10 seconds
```

Step 4: Consume Kafka messages and update PostgreSQL table
```python
import json

# Consume Kafka messages, update PostgreSQL table
def consume_kafka_and_update_postgres():
    for message in consumer:
        user_data = json.loads(message.value.decode('utf-8'))

        # Insert the user_data into the PostgreSQL table
        # Adapt the code to construct the INSERT query and execute it using psycopg2
```

You can integrate these code snippets into your project and modify them as per your requirements. Remember to install the required dependencies (`kafka-python`, `psycopg2`) using `pip` before running the code.


import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1,"
                                           "org.apache.commons:commons-pool2:2.8.0,"
                                           "org.apache.kafka:kafka-clients:2.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview2,"
                                           "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.0.0-preview2") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        print('Spark connection created successfully!')
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
        print("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    print('I am here')
    sel = spark_df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()
