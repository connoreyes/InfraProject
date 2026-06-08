from kafka import KafkaConsumer
from json import loads
import psycopg2
from dotenv import load_dotenv
load_dotenv()

consumer = KafkaConsumer(
   'solar-flares', # subscribes to topic called test
    auto_offset_reset='earliest', # when it connects, read from the very first message stored
    enable_auto_commit=True, # auto save each offset after each message is read
    group_id='my-group-2', #  identifies this consumer as part of a group; Kafka uses this to track what's been read
    value_deserializer=lambda m: loads(m.decode('utf-8')), # Kafka stores bytes, this converts them back to a Python dict via JSON
    bootstrap_servers=['localhost:9092']) # the three addresses of your Kafka brokers running in Docker

conn = psycopg2.connect(
    dbname="spaceweather",
    user= os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    host="localhost",
    port="5432"
)

cur = conn.cursor()
# infinite loop, blocks and waits for new messages in consumer
for m in consumer:
    cur.execute(
        "INSERT INTO solar_flares (flr_id, class_type, begin_time, peak_time, end_time, source_location) Values(%s, %s, %s, %s, %s, %s)",  
        (m.value['flrID'], m.value['classType'], m.value['beginTime'], m.value['peakTime'], m.value['endTime'], m.value['sourceLocation'])
    )
    conn.commit()
    # print every message 
    