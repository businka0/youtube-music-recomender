import urllib3
import json
import ydb
import os
import string
import random
import numpy as np
import datetime
from ytmusicapi import YTMusic

ytmusic = YTMusic()
http = urllib3.PoolManager(retries=False)
MOOD_CATEGORIES = [
    'Chill',
    'Commute',
    'Energy Boosters',
    'Feel Good',
    'Focus',
    'Party',
    'Romance',
    'Sad',
    'Sleep',
    'Workout'
]

TG_TOKEN=os.getenv('TG_TOKEN')
URL = f"https://api.telegram.org/bot{TG_TOKEN}/"

driver_config = ydb.DriverConfig(
    endpoint=os.getenv('YDB_ENDPOINT'),
    database=os.getenv('YDB_DATABASE'),
    credentials=ydb.iam.MetadataUrlCredentials()
)

driver = ydb.Driver(driver_config)
driver.wait(fail_fast=True, timeout=5)
pool = ydb.SessionPool(driver)

def search(name, test_list):
    return [element for element in test_list if element['title'] == name]

def randomword(length=10):
	letters = string.ascii_lowercase
	return ''.join(random.choice(letters) for i in range(length))

def execute_query(text):
    return pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))


def meme_by_arm(cls, session):
    text = f'''
        SELECT id, cls FROM memes
        WHERE cls == '{cls}'
            AND id not in (SELECT meme_id FROM answers WHERE session_id == CAST('{session}' AS Utf8))
        ORDER BY random(id)
        LIMIT 1;
    '''

    text_except = f'''
        SELECT id, cls FROM memes
        WHERE cls == '{cls}'
        ORDER BY random(id)
        LIMIT 1;
    '''

    meme_by_arm_result = execute_query(text)[0].rows
    if meme_by_arm_result == [] :
      meme_by_arm_result = execute_query(text_except)[0].rows

    return meme_by_arm_result

def select_all(tablename):
    text = f"SELECT * FROM {tablename};"
    return execute_query(text)

def insert_log(tablename, cls, ans, meme, session):
     text = f"INSERT INTO {tablename} SELECT CAST('{randomword()}' as Utf8?) as id,  CAST('{cls}' as Utf8?) as cls, {ans} as answer, CAST('{meme}' as Utf8?) as meme_id, CAST('{session}' as Utf8) as session_id;"
     return execute_query(text)

def select_count_cls(tablename, cls, session):
    text = f"SELECT COUNT(*) as hits FROM {tablename} WHERE cls == '{cls}' and session_id == CAST('{session}' AS Utf8);"
    return execute_query(text)

def select_best_cls(tablename, session):
    text = f"SELECT cls, COUNT_IF(answer) / COUNT(*) as hits,COUNT_IF(answer) as n_ans  FROM {tablename} WHERE session_id == CAST('{session}' AS Utf8) GROUP BY cls ORDER BY hits DESC LIMIT 1;"
    return execute_query(text)

def send_message(text, chat_id):
    url = URL + f"sendMessage?text={text}&chat_id={chat_id}"
    http.request("GET", url)

def create_keyboard(arm):
	return json.dumps(
		{
			"inline_keyboard":[
				[{ "text": "👎", "callback_data": f"{arm}_no"},
				{ "text": "❤️", "callback_data": f"{arm}_yes"}]
			]
		}
	)

choiceText = "Нравится этот мем?"

def send_pic(text, chat_id, session):
    # Select meme by arm, random but not previously sent in chat
    final_text = meme_by_arm(text, session)
    print(f'final_text: {final_text}')
    url = URL + f"sendMessage?text={final_text[0]['cls']}&chat_id={chat_id}"
    cls = final_text[0]['cls']
    print(cls)
    photo_id = str(final_text[0]['id'])
    print(photo_id)
    # quote(unicodedata.normalize('NFC', cls))
    meme_photo = f"https://storage.yandexcloud.net/test-buckets/music-chat-bot/{cls}/{photo_id}"
    print(meme_photo)
    url2 = URL + f"sendPhoto?photo={meme_photo}&chat_id={chat_id}"
    print(url2)
    http.request("GET", url2)
    systime = str(datetime.datetime.now())
    s = systime[0:20].replace(' ', 'T', 1).replace('.', 'Z', 1)
    text = f"INSERT INTO memes_shown SELECT CAST('{randomword()}' as Utf8?) as id,  CAST('{cls}' as Utf8?) as cls, CAST('{photo_id}' as Utf8?) as meme_id, CAST('{session}' as Utf8) as session_id, datetime('{s}') as systime;"
    execute_query(text)

def send_question(arm, chat_id):
  	# Create data dict
	data = {
		'text': (None, choiceText),
		'chat_id': (None, chat_id),
		'parse_mode': (None, 'Markdown'),
		'reply_markup': (None, create_keyboard(arm))
	}
	url = URL + "sendMessage"
	http.request(
		'POST',
		url,
		fields=data
	)

def send_feedback(reply, chat_id, cbq_id):
    url = URL + f"answerCallbackQuery?callback_query_id={cbq_id}"
    http.request("GET", url)

class eGreedy:
    def __init__(self, arms=MOOD_CATEGORIES, e=0.01):
        self.arms = arms
        self.e = e

    def set_session_id(self, session):
        self.session = session

    def decide(self):
        # дернуть каждую ручку один раз для первоначальной статистики
        for arm in self.arms:
            if select_count_cls('answers', arm, self.session)[0].rows[0]['hits'] == 0:
                print(f'Дергаю неинициализированную ручку {arm}')
                return arm

        # если случайное число меньше epsilon, то выбрать ручку случайно и закончить
        if np.random.rand() < self.e:
            print('Случайный выбор ручки!')
            return random.choice(self.arms)
        print('Осознанный выбор лучшей ручки')
        cls_selector = select_best_cls('answers', self.session)[0].rows[0]
        best_cls = cls_selector['cls']
        best_cls_n = cls_selector['n_ans']

        if best_cls_n >=2 :
          return(f'Мы выбрали плейлист, который идеально пододйет под ваше настроение:{best_cls}')

        # в качестве ответа выбрать ту ручку, у которой наибольшее среднее
        return best_cls

    # обновить историю полученных наград у ручки arm
    def update(self, arm, reward):

        text2 = f"SELECT meme_id, systime FROM memes_shown WHERE session_id == CAST('{self.session}' AS Utf8) ORDER BY systime DESC LIMIT 1;"
        result_query = execute_query(text2)
        print(result_query[0])
        if result_query[0] == []:
          return
        else:
          last_meme_id = result_query[0].rows[0]['meme_id']
          print(last_meme_id)
          insert_log('answers', arm, reward, last_meme_id, self.session)

    def get_next(self, chat_id):
        # выбираем ручку
        arm = self.decide()
        if arm.startswith('Мы'):
          mood = arm.split(":",1)[1]
          print(mood)
          categories = ytmusic.get_mood_categories()['Moods & moments']
          specific_category = search(mood, categories)
          print(specific_category)
          specific_collection = ytmusic.get_mood_playlists(specific_category[0]['params'])
          random_playlist = random.choice(specific_collection)['playlistId']# случайно выбираем плейлист из коллекции
          random_link = 'https://music.youtube.com/playlist?list='+ (random_playlist) # формируем ссылку на плейлист

          text = f'Подобранный плейлист: {random_link}'
          send_message(text,chat_id ) # отпраялем сообщение

          #записываем в базу
          text2 = f"INSERT INTO rec_playlists SELECT CAST('{randomword()}' as Utf8?) as id,  CAST('{mood}' as Utf8?) as cls, CAST('{chat_id}' AS int64)as chat_id, CAST('{random_link}' AS String) as link;"
          execute_query(text2)

          text3 = f"UPDATE sessions SET is_active = CAST('false' as Bool) WHERE id == CAST('{self.session}' AS Utf8);"
          execute_query(text3)
          else_message = "У меня также есть другие функции, можешь ими воспользоваться:\n\n/nsend_random_playlist -- предложу рандомный плейлист из Youtube музыки\n\n/send_rec_and_memes -- пришлю тебе мемы, а на их основе рекомендацию плейлиста по настроению\n\n/send_report_on_rec --  пришлю информацию по твоему настроению на основе реакции на мемы за последнее время\n\n/send_report_rec_history -- покажу историю порекомендованных плейлистов»."
          send_message(else_message, chat_id)
          return

        else:
          print(f'Selected arm: {arm}')
          # т.к. send_pic() в качестве параметра принимает текст, то преобразуем к тексту. Можно поменять send_pic и сделать выбор API по числу
          send_pic(arm, chat_id, self.session)
            # отправляем сразу и меню
          send_question(arm, chat_id)

#функция, которая выдает ссылку на плейлист из случайной категории
def send_random_playlist(chat_id):
  random_ganre = ['ggMPOg1uX1JOQWZFeDByc2Jm',
                'ggMPOg1uX044Z2o5WERLckpU',
                'ggMPOg1uX2lRZUZiMnNrQnJW',
                'ggMPOg1uXzZQbDB5eThLRTQ3',
                'ggMPOg1uX0NvNGNhWThMYWRh',
                'ggMPOg1uX2w1aW1CRDFTSUNo',
                'ggMPOg1uX1JCQnB2QXVYVEIz',
                'ggMPOg1uX3NISTh4UmtWcFgz',
                'ggMPOg1uX1MxaFQ3Z0JMZkN4',
                'ggMPOg1uXzIxYkNac21YZ2Z0']
  random_collection = ytmusic.get_mood_playlists(random.choice(random_ganre))# выбираем коллекцию с плейлистами случайного жанра
  random_playlist = random.choice(random_collection)['playlistId']# случайно выбираем плейлист из коллекции
  random_link = 'https://music.youtube.com/playlist?list='+ (random_playlist) # формируем ссылку на плейлист
  text = f'Подобранный плейлист: {random_link}'
  chat__id = chat_id
  send_message(text,chat_id ) # отпраялем сообщение
  return

def handler(event, context):
    print(f"event == {json.dumps(event)}")
    message = json.loads(event['body'])

    if 'callback_query' in message.keys():
      # получен ответ на меню
      arm, reply = message['callback_query']['data'].split('_')
      chat_id = message['callback_query']['message']['chat']['id']
      cbq_id = message['callback_query']['id']
      # подтвердим, что было выбрано
      send_feedback(reply, chat_id, cbq_id)
      
      # обновляем
      text = f"SELECT id, systime FROM sessions WHERE chat_id == {chat_id} ORDER BY systime desc LIMIT 1"
      session_id = execute_query(text)[0].rows[0]['id']
      print(f'Session: {session_id}')
      egreedy_policy.set_session_id(session_id)
      egreedy_policy.update(arm, reply == 'yes')
      egreedy_policy.get_next(chat_id)


    else:
      chat_id = message['message']['from']['id']
      reply = message['message']['text']

      # Команда /start
      if reply == '/start':
        start_message = "Привет!\n\nЭто бот для подбора музыки по твоему настроению! На данный момент он поддерживает следующие команды:\n\n/send_random_playlist -- предложу рандомный плейлист из Youtube музыки\n\n/send_rec_and_memes -- пришлю тебе мемы, а на их основе рекомендацию плейлиста по настроению\n\n/send_report_on_rec --  пришлю информацию по твоему настроению на основе реакции на мемы за последнее время\n\n/send_report_rec_history -- покажу историю порекомендованных плейлистов»."
        send_message(start_message, chat_id)

      # Команда /send_random_playlist
      elif reply == '/send_random_playlist':
        send_random_playlist(chat_id)
        else_message = "У меня также есть другие функции, можешь ими воспользоваться:\n\n/send_random_playlist -- предложу рандомный плейлист из Youtube музыки\n\n/send_rec_and_memes -- пришлю тебе мемы, а на их основе рекомендацию плейлиста по настроению\n\n/send_report_on_rec --  пришлю информацию по твоему настроению на основе реакции на мемы за последнее время\n\n/send_report_rec_history -- покажу историю порекомендованных плейлистов»."
        send_message(else_message, chat_id)

      # Команда /send_rec_and_memes
      elif reply == '/send_rec_and_memes':
        chat_id = message['message']['chat']['id']
        systime = str(datetime.datetime.now())
        s = systime[0:20].replace(' ', 'T', 1).replace('.', 'Z', 1)
        session_id = f'{randomword()}{chat_id}'
        print(f'Session started: {session_id}')
        is_active = 'true'
        text = f"INSERT INTO sessions SELECT CAST('{session_id}' as Utf8?) as id,  {chat_id} as chat_id, datetime('{s}') as systime, true as is_active;"
        execute_query(text)
        egreedy_policy.set_session_id(session_id)
        else_message = "/send_rec_and_memes: начало сессии"
        send_message(else_message, chat_id)
        egreedy_policy.get_next(chat_id)

      elif reply == '/send_report_rec_history':
        text2 = f"SELECT link FROM rec_playlists WHERE chat_id == CAST('{chat_id}' AS int64);"
        result_query = execute_query(text2)
        print(result_query[0])
        print(result_query[0].rows[0])
        result_string = ""
        for result in result_query[0].rows:
          result_string += f"- {result['link']}\n"
        report_rec_history_message = f"Это история твоих рекомендаций🎵\n\n{result_string}"
        send_message(report_rec_history_message, chat_id)

      elif reply == '/send_report_on_rec':
        MOOD_CATEGORIES_MAP = {
            'Chill' : 'Чилл',
            'Commute' : 'Поездки мои поездки',
            'Energy Boosters' : 'Энергичность',
            'Feel Good' : 'Хорошее настроение',
            'Focus' : 'Концентрация',
            'Party' : 'Тусовка',
            'Romance' : 'Романтика',
            'Sad' : 'Грусть',
            'Sleep' : 'Сон',
            'Workout' : 'Воркаут'
        }
        text2 = f"SELECT cls FROM rec_playlists WHERE chat_id == CAST('{chat_id}' AS int64);"
        result_query = execute_query(text2)
        print(result_query[0])
        print(result_query[0].rows[0])
        result_string = ""
        for result in result_query[0].rows:
          result_string += f"- {MOOD_CATEGORIES_MAP[result['cls']]}\n"
        report_rec_history_message = f"Это история твоего настроения по реакциям на мемы\n\n{result_string}"
        send_message(report_rec_history_message, chat_id)

    #text_to_send = "Я бы рад поболтать, но пока не умею:(\n\Выбери, пожалуйста, команду, которую я смогу обработать!"
    #send_message(chat_id, text_to_send)

    return {
        'statusCode': 200
    }

egreedy_policy = eGreedy(e=0.2)
