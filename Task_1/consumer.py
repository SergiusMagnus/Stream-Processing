import json
from datetime import datetime, timedelta
from typing import NoReturn

import pandas as pd
from confluent_kafka import Consumer

TOPIC_NAME = 'sensors_topic'

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}


def print_stats(data: list[dict]):
    df = pd.DataFrame(data)

    print('Mean by sensor name:')
    print(df.groupby('name')['value'].mean().to_string(header=False))
          
    print('\nMean by sensor type:')
    print(df.groupby('type')['value'].mean().to_string(header=False))


def main(delay: float) -> NoReturn:
    consumer = Consumer(config)
    consumer.subscribe([TOPIC_NAME])

    msgs = []
    end_time = datetime.now() + timedelta(seconds=delay)

    try:
        while True:
            msg = consumer.poll(1)

            if msg:
                msgs.append(json.loads(msg.value()))

            if datetime.now() > end_time:
                print('\n', '=' * 50, '\n')

                if msgs:
                    print_stats(msgs)
                    msgs = []
                else:
                    print('No data.')

                end_time = datetime.now() + timedelta(seconds=delay)

    finally:
        consumer.close()

if __name__ == '__main__':
    main(5)