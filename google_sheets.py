from datetime import datetime
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials

import psycopg2
from config import host, user, password, db_name

from pycbrf.toolbox import ExchangeRates

CREDENTIALS_FILE = 'notional-buffer-361819-360ef4db9af6.json'  # Имя файла с закрытым ключом, вы должны подставить свое

# Читаем ключи из файла
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE,
    [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
)

httpAuth = credentials.authorize(httplib2.Http())  # Авторизуемся в системе
service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)  # Выбираем работу с таблицами и 4 версию API

spreadsheetId = "1y-goRucJZJeSA1STEk9BvQG8u23rdwjKMP8afSycicM"  # Id документа с заказами и стоимостью
print('https://docs.google.com/spreadsheets/d/' + spreadsheetId)


# Получаем список листов, их Id и название
spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheetId).execute()
sheetList = spreadsheet.get('sheets')

sheetId = sheetList[0]['properties']['sheetId']

# Получение информации из стобика с ценой в долларах
results_dollars_get = service.spreadsheets() \
                             .values() \
                             .batchGet(
                                 spreadsheetId=spreadsheetId,
                                 ranges='C2:C1000',
                                 dateTimeRenderOption='FORMATTED_STRING'
                             ).execute()


# Перевод из рубли в доллары по курсу цб рф на сегодня
def _change_dollars_to_rubles(dollars_values):
    cost_rubles = dollars_values['valueRanges'][0]['values']
    date_today = datetime.today().strftime('%Y-%m-%d')
    rates = ExchangeRates(date_today)
    dollar_exchange = rates['USD'].value
    for value in cost_rubles:
        value[0] = str(int(value[0]) * dollar_exchange)
    return cost_rubles


# Получение информации из всей таблицы
table_get = service.spreadsheets() \
                   .values() \
                   .batchGet(
                       spreadsheetId=spreadsheetId,
                       ranges='A2:E1000',
                       dateTimeRenderOption='FORMATTED_STRING',
                   ).execute()
table_test = table_get['valueRanges'][0]['values']

numbers_get = service.spreadsheets() \
                   .values() \
                   .batchGet(
                       spreadsheetId=spreadsheetId,
                       ranges='A2:A1000',
                       dateTimeRenderOption='FORMATTED_STRING',
                   ).execute()
numbers_test_get = sum(numbers_get['valueRanges'][0]['values'], [])

results = service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheetId, body={
    "valueInputOption": "USER_ENTERED",  # Данные воспринимаются, как вводимые пользователем (считается значение формул)
    "data": [
        {"range": "E2:E1000",
         "values": _change_dollars_to_rubles(results_dollars_get)}
    ]
}).execute()


# Подключение к бд
connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=db_name
)
connection.autocommit = True
try:
    # заполнение данных в бд и их обновление
    with connection.cursor() as cursor:
        for row in table_test:
            cursor.execute(
                f"""INSERT INTO test (number, order_number, cost_dollars, delivery_time, cost_rubles)
                    VALUES
                        (%s, %s, %s, %s, %s)
                    ON CONFLICT (number) DO UPDATE
                    SET
                        order_number = {row[1]},
                        cost_dollars = {row[2]},
                        delivery_time = '{row[3].replace('.', '-')}',
                        cost_rubles = {row[4]}""", tuple(row)
            )
        print("[INFO] table update successfuly")


    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT number FROM test"""
        )
        db_test_numbers = cursor.fetchall()
        db_test_numbers_ = []
        for number in db_test_numbers:
            db_test_numbers_.append(str(number[0]))

    with connection.cursor() as cursor:
        for number in db_test_numbers_:
            try:
                numbers_test_get.index(number)
            except:
                cursor.execute(
                    f"""DELETE from test
                    WHERE
                        number = {number}"""
                )
                print('[INFO]Удалённые значения в таблице были удалены из бд')

finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection close")

