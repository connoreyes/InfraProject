from kafka import KafkaProducer
import json
import requests
import time

API_KEY = 'Hn0AspjseqcFAMniudnHhdzxXKZ4E90y733JuZok'
URL = f'https://api.nasa.gov/DONKI/FLR?startDate=2024-01-01&endDate=2024-12-31&api_key={API_KEY}'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


while True:
    response = requests.get(URL)
    flares = response.json()
    
    for flare in flares:
        producer.send('solar-flares', flare)
        print(f"Sent: {flare.get('flrID')} - Class: {flare.get('classType')}")
    
    producer.flush()
    print(f"Sent {len(flares)} flares, sleeping 60s...")
    time.sleep(60)