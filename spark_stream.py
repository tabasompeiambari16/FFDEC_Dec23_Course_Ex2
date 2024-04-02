import requests
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime
import time
import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

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


      # Step 2: PostgreSQL Setup and Table Creation

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

    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
      
      # Step 3: Fetch data from Random User Generator API and send to Kafka

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


      #Step 4: Consume Kafka messages and update PostgreSQL table

import json

      # Consume Kafka messages, update PostgreSQL table
      
def consume_kafka_and_update_postgres():
    for message in consumer:
        user_data = json.loads(message.value.decode('utf-8'))

        # Insert the user_data into the PostgreSQL table
        # Adapt the code to construct the INSERT query and execute it using psycopg2


      #You can integrate these code snippets into your project and modify them as per your requirements.
