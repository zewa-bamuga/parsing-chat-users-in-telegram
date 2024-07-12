import csv
import asyncio
import os
import time

from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerUser, PeerChannel


async def fetch_messages_and_participants(client, target_group, limit=1100):
    offset_id = 0
    all_messages = []
    total_messages = 0
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
        user_ids.update({message.from_id.user_id for message in messages if isinstance(message.from_id, PeerUser)})
        all_messages.extend([message.message for message in messages])
        offset_id = messages[-1].id
        total_messages += len(messages)
        if total_messages >= limit:
            break

    all_participants = await client.get_participants(target_group)
    participants_dict = {user.id: user for user in all_participants if user.id in user_ids}

    return user_ids, participants_dict


async def parse_telegram_chat(api_id, api_hash, phone, chat_url, limit=1100):
    async with TelegramClient(phone, api_id, api_hash) as client:
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
                if entity.broadcast:
                    target_group = entity
                elif entity.megagroup:
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

        user_ids, participants_dict = await fetch_messages_and_participants(client, target_group, limit=limit)

        target_title = target_group.title if hasattr(target_group, 'title') else 'Unknown Group'

        return user_ids, participants_dict, target_title


async def main():
    api_id =
    api_hash = ""
    phone = ""

    start_time = time.time()

    current_directory = os.getcwd()
    print("Текущая директория проекта:", current_directory)

    print("Введите ссылку на чат или канал для парсинга сообщений и членов группы:")
    chat_url = input("Введите ссылку: ")

    try:
        user_ids, participants_dict, target_title = await parse_telegram_chat(api_id, api_hash, phone, chat_url)

        if participants_dict:
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
                        writer.writerow([username, name, target_title])

            print("Парсинг активных участников группы успешно выполнен.")
        else:
            print("Нет доступных участников для записи в CSV.")

        print(f"Время выполнения парсинга: {time.time() - start_time:.2f} секунд")

    except ValueError as ve:
        print(f"Ошибка: {ve}")


if __name__ == "__main__":
    asyncio.run(main())
