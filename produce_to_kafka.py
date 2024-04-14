import requests
from kafka import KafkaProducer
import json
import time

API_KEY = input('Please Enter API Key:')


def fetch_weather_data(city, api_key):
    time.sleep(2)
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def produce_to_kafka(data, topic, kafka_servers):
    producer = KafkaProducer(bootstrap_servers=kafka_servers,
                             # Serialization is necessary because Kafka is a distributed system, and messages must be converted into bytes before they can be transmitted
                             value_serializer=lambda x: json.dumps(x).encode('utf-8')) 
    producer.send(topic, value=data)
    producer.flush()
    producer.close()

# raw_data = [fetch_weather_data(row,api_key=API_KEY) for row in cities]
    

def filter_open_weather_api_data(input_data:dict):
    city_name = input_data['name']
    weather_id = input_data['weather'][0]['id']
    weather_description = input_data['weather'][0]['description']
    temperature = input_data['main']['temp']
    pressure = input_data['main']['pressure']
    humidity = input_data['main']['humidity']
    wind_speed = input_data['wind']['speed']
    clouds = input_data['clouds']['all']
    timestamp = input_data['dt']
    timezone = input_data['timezone']

    dict_1 = {'city_name':city_name,
            'weather_id':weather_id,
            'weather_description':weather_description,
            'temperature':temperature,
            'pressure':pressure,
            'humidity':humidity,
            'wind_speed':wind_speed,
            'clouds':clouds,
            'created_at':timestamp,
            'timezone':timezone
            }
    return dict_1


topic = 'weather-data-1'
kafka_servers = 'localhost:9092'

cities = ['Delhi','Mumbai','Bengaluru','Kolkata','Hyderabad','Chennai']


while True:

    for row in cities:
        api_data = fetch_weather_data(row,api_key = API_KEY)
        filtered_api_data = filter_open_weather_api_data(api_data)
        # print(api_data)
        produce_to_kafka(data = filtered_api_data,topic = topic,kafka_servers = kafka_servers)
    # break
