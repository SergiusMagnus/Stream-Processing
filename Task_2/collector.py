import asyncio
import configparser
from datetime import datetime, timezone, timedelta

import fastparquet
import pandas as pd
from telethon import TelegramClient, events

async def main(delay: float, api_id: int, api_hash: str):
    posts = []
    lock = asyncio.Lock()

    async with TelegramClient('task2', api_id, api_hash) as client:
        channels = []
        channels.append(client.get_entity('t.me/readovkanews'))
        channels.append(client.get_entity('t.me/bloodysx'))
        channels.append(client.get_entity('t.me/ostorozhno_novosti'))
        channels.append(client.get_entity('t.me/breakingmash'))
        channels.append(client.get_entity('t.me/rian_ru'))
        channels.append(client.get_entity('t.me/nexta_live'))
        # channels.append(await client.get_entity('t.me/sp_task2'))

        @client.on(events.NewMessage(chats=channels))
        async def handler(event):
            tz = timezone(timedelta(hours=3))
            post = {
                'time': str(event.date.astimezone(tz).time()),
                'channel_name': str(event.chat.username),
                'text': event.raw_text,
                'media': True if event.media else False
            }
            async with lock:
                posts.append(post)

        while True:
            start_time = datetime.now()
            await asyncio.sleep(delay)

            async with lock:
                df = pd.DataFrame(posts)
                posts = []

            end_time = datetime.now()
            filename = f'{start_time.time().isoformat(timespec="seconds")}_{end_time.time().isoformat(timespec="seconds")}'.replace(':', '-')
            fastparquet.write(f'Task_2/data/{filename}.parq', df)

if __name__ == '__main__':
    cfg = configparser.ConfigParser()
    cfg.read('Task_2/properties.ini')

    asyncio.run(main(5, cfg['Telegram']['api_id'], cfg['Telegram']['api_hash']))