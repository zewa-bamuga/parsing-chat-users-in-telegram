import csv
import asyncio
import os
import time
from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerUser, PeerChannel

load_dotenv()

api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
phone = os.getenv('PHONE')

async def main():
    start_time = time.time()
    client = TelegramClient(phone, api_id, api_hash)
    await client.start()

    print("Введите ссылку на чат или канал для парсинга сообщений и членов группы:")
    chat_url = input("Введите ссылку: ")

    # Извлечение идентификатора чата или канала из ссылки
    if 'https://web.telegram.org/a/#' in chat_url:
        chat_id = chat_url.split('#')[-1]
        if chat_id.startswith('-'):
            chat_id = int(chat_id)
        else:
            chat_id = int(f'-100{chat_id}')
    elif 'https://t.me/' in chat_url:
        username = chat_url.split('/')[-1]
        try:
            entity = await client.get_entity(username)
            if entity.broadcast:  # Это канал
                target_group = entity
            elif entity.megagroup:  # Это мега-группа
                target_group = entity
            else:
                print("Неподдерживаемый тип чата или канала")
                return
        except Exception as e:
            print(f"Не удалось получить информацию о чате или канале: {e}")
            return
    else:
        print("Неправильный формат ссылки")
        return

    try:
        if not 'target_group' in locals():
            target_group = await client.get_entity(PeerChannel(chat_id))
    except Exception as e:
        print(f"Не удалось получить информацию о чате или канале: {e}")
        return

    print("Узнаём пользователей, которые писали сообщения...")

    offset_id = 0
    limit = 10
    all_messages = []
    total_messages = 0
    total_count_limit = 10
    user_ids = set()

    while True:
        history = await client(GetHistoryRequest(
            peer=target_group,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            if isinstance(message.from_id, PeerUser):
                user_ids.add(message.from_id.user_id)
            all_messages.append(message.message)
        offset_id = messages[-1].id
        total_messages += len(messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    print("Сохраняем данные о пользователях в файл...")

    all_participants = await client.get_participants(target_group)
    participants_dict = {user.id: user for user in all_participants if user.id in user_ids}

    with open("active_members.csv", "w", encoding="UTF-8") as f:
        writer = csv.writer(f, delimiter=",", lineterminator="\n")
        writer.writerow(["username", "name", "group"])
        for user_id in user_ids:
            user = participants_dict.get(user_id)
            if user:
                username = user.username if user.username else ""
                first_name = user.first_name if user.first_name else ""
                last_name = user.last_name if user.last_name else ""
                name = (first_name + ' ' + last_name).strip()
                writer.writerow([username, name, target_group.title])

    print("Парсинг активных участников группы успешно выполнен.")

    print("Сохраняем данные о сообщениях в файл...")
    with open("chats.csv", "w", encoding="UTF-8") as f:
        writer = csv.writer(f, delimiter=",", lineterminator="\n")
        for message in all_messages:
            writer.writerow([message])

    print('Парсинг сообщений группы успешно выполнен.')
    print(f"Время выполнения парсинга: {time.time() - start_time:.2f} секунд")

asyncio.run(main())