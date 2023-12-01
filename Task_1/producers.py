import asyncio
import json
import time
from typing import NoReturn

import numpy as np
from confluent_kafka import Producer

TOPIC_NAME = 'sensors_topic'

def gen_sensor_value(name: str, sensor_type: str, mu: float, sigma: float) -> dict[str, str|float]:
    return {
        'name': name,
        'type': sensor_type,
        'value': np.random.normal(mu, sigma),
        'timestamp': time.time()
    }

async def emulate_sensor(name: str, sensor_type: str, delay: float, mu: float, sigma: float) -> NoReturn:
    config = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(config)
    
    while True:
        producer.produce(TOPIC_NAME, key=name, value=json.dumps(gen_sensor_value(name, sensor_type, mu, sigma)), timestamp=int(time.time()) * 1000)
        producer.poll(10)
        await asyncio.sleep(delay)

async def main(sensors_setup_data) -> NoReturn:
    tasks = [asyncio.create_task(emulate_sensor(*data)) for data in sensors_setup_data]
    await asyncio.gather(*tasks)

sensors_setup_data = [
    # sensor_name sensor_type delay mean standard_deviation
    ['sensor_T1', 'temperature', 5.1, 12.0, 1.0],
    ['sensor_T2', 'temperature', 2.0, 13.0, 1.0],
    ['sensor_T3', 'temperature', 3.4, 14.0, 1.0],
    ['sensor_T4', 'temperature', 20.3, 13.8, 1.0],

    ['sensor_P1', 'pressure', 10.2, 750.0, 3.0],
    ['sensor_P2', 'pressure', 6.7, 750.0, 2.0],
    ['sensor_P3', 'pressure', 13.0, 750.0, 1.0],

    ['sensor_H1', 'humidity', 4.1, 90.0, 3.0],
    ['sensor_H2', 'humidity', 6.3, 67.0, 1.0],
    ['sensor_H3', 'humidity', 8.8, 80.0, 2.0],
]

if __name__ == '__main__':
    asyncio.run(main(sensors_setup_data))