import requests
import json
import psycopg2
import datetime as dt
# При подключении скрипта через AirFlow раскомментировать
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator

# параметры дага
args = {
    'owner': 'itmo_student',
    'start_date': dt.datetime(2025, 12, 1),
}

# параметры бд
host = "192.168.*.*"
port = 5432
database = "meteo"
user = "airflow"
password = "airflow"
# устанавливаем соединение с бд
conn = psycopg2.connect(host = host, port = port, database = database, user = user, password=password)
conn.autocommit = True
cur = conn.cursor()

# параметры запроса
url_weather = 'https://api.openweathermap.org/data/2.5/weather?' 
url_forecast = 'https://api.openweathermap.org/data/2.5/forecast?'
appid =   
lat = 59.9                      # широта СПб
lon = 30.3                      # долгота СПб
# исключения без пробелов через запятую current,minutely,hourly,daily,alerts
exclude = 'minutely,alerts'     
lang = 'ru'                     # язык для комментариев
# 'standard' - Кельвин, 'metric' - Цельсий, 'imperial' - Фаренгейт.
units = 'metric'                
params = {'lat': lat, 'lon': lon, 'APPID': appid, 'exclude': exclude, 'lang': lang, 'units': units}

def load_current_weather(): # загрузка с API текущей погоды        
    
    try: 
        # получаем данные с API 
        response = requests.get(url = url_weather, params = params, timeout = 5)
    except Exception as ex:
            print("Ошибка подключения: ", ex)
            quit()

    data = response.json()
    to_json = json.dumps(data)
    print("Получен JSON с текущей погодой {:>30}".format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    # print(to_json)

    проверяем, есть ли запись в current_weather за полученную дату
    date_current = dt.datetime.fromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("SELECT id FROM meteo.current_weather WHERE current_weather.date = '{}'".format(date_current))
    # если такой записи нет - добавляем
    if len(cur.fetchall()) > 0:
        print("В current_weather запись существует {:>25}".format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    else:
        # вызов хранимки add_current_weather 
        cur.execute("call meteo.add_current_weather('{}')".format(to_json))
        print("Запись в current_weather добавлена {:>26}".format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


def load_forecast_hourly(): # загрузка с API почасового прогноза

    try:
        # получаем данные с API 
        response = requests.get(url = url_forecast, params = params, timeout = 5)  
    except Exception as ex:
        print("Ошибка получения: ", ex)
        quit()

    data = response.json()
    to_json = json.dumps(data)
    print("Получен JSON с прогнозной погодой {:>27}".format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    # вызов хранимки add_forecast_hourly 
    cur.execute("call meteo.add_forecast_hourly('{}')".format(to_json))
    # total_count = cur.fetchone()
    # print(total_count)
    print("Записи в forecast_hourly обновлены {:>26}".format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

# При подключении скрипта через AirFlow раскомментировать
# with DAG(dag_id='its_load_weather', default_args=args, schedule_interval='*/7 * * * *', catchup = False) as dag:
#     load_current_weather = PythonOperator(task_id='load_current_weather', python_callable=load_current_weather)
#     load_forecast_hourly = PythonOperator(task_id='load_forecast_hourly', python_callable=load_forecast_hourly)

# load_current_weather >> load_forecast_hourly

# выполняется из Питона
# При подключении скрипта через AirFlow закомментировать
load_current_weather()
load_forecast_hourly()
