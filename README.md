# Описание кода для прогнозирования уровня топлива на АЗС
Данный скрипт предназначен для прогнозирования потребления топлива на автозаправочной станции (АЗС) и определения момента, когда уровень топлива достигнет мертвого остатка. Он использует исторические данные из базы данных Oracle и библиотеку Prophet для создания прогноза.
## Импорт необходимых библиотек
```python
import pandas as pd
import matplotlib.pyplot as plt
import logging
from prophet import Prophet
from sqlalchemy import create_engine
import cx_Oracle
import sys
```

## Инициализация Oracle Instant Client
```python
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\aalik\Documents\instantclient_23_4")
```
Здесь мы инициализируем Oracle Instant Client, указав путь к библиотеке.

##Настройка логирования
```python
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
```
Конфигурируем логирование для отображения информации о процессе выполнения и возможных ошибках.

## Параметры подключения к базе данных
```python
username = 'Ваш логин'  # Оставьте ваш логин
password = 'Ваш пароль'  # Введите ваш пароль
host = 'Ваш адерсс'
port = 1521
service_name = 'ORCL'
```
Здесь задаются параметры для подключения к базе данных Oracle.

## Создание строки подключения и движка SQLAlchemy
```python
dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
engine = create_engine(f'oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service_name}')
```
Создаем DSN и движок для взаимодействия с базой данных через SQLAlchemy.

## Словарь соответствия названий топлива и GASNUM
```python
fuel_mapping = {
    'Аи-80': '3300000000',
    'Аи-92': '3300000002',
    'Аи-95': '3300000005',
    'Аи-98': '3300000008',
    'ДТ-Л': '3300000010',
    'ДТ-3-25': '3300000029',
    'ДТ-3-32': '3300000038',
    'СУГ': '3400000000',
    'Аи-95-import': '3300000095'
}
```
Этот словарь используется для преобразования названия топлива в соответствующий код GASNUM.

## Получение ввода от пользователя
```python
object_code = input("Введите номер АЗС (OBJECTCODE): ")  # Например, 'Z313'
```
Пользователь вводит код АЗС.

## Вывод доступных видов топлива и выбор пользователем
```python
print("Доступные виды топлива:")
for fuel_name in fuel_mapping.keys():
    print(f"- {fuel_name}")

fuel_name = input("Введите название топлива из списка выше: ")  # Например, 'Аи-92'
```
Выводим список доступных видов топлива и запрашиваем у пользователя выбор.

## Проверка корректности введенного топлива и получение GASNUM
```python
if fuel_name not in fuel_mapping:
    print("Введено некорректное название топлива. Пожалуйста, выберите из списка.")
    sys.exit(1)

gasnum = fuel_mapping[fuel_name]
```
Проверяем, что введенное название топлива есть в словаре, и получаем соответствующий GASNUM.

## SQL-запрос для получения данных
```python
query = """
SELECT
    TRUNC(R_DAY) AS ДАТА,
    R_HOUR,
    GASNUM AS PRODNAME,
    TANK,
    NVL(VOLUME, 0) AS КОЛИЧЕСТВО
FROM
    bi.avtozakazy
WHERE
    OBJECTCODE = :object_code AND
    GASNUM = :gasnum AND
    TANK = 3
"""
```
Формируем SQL-запрос для выборки данных из базы.

## Загрузка данных в DataFrame
```python
try:
    df = pd.read_sql(query, con=engine, params={'object_code': object_code, 'gasnum': gasnum})
except Exception as e:
    logging.error(f"Ошибка при чтении данных из базы данных: {e}")
    sys.exit(1)
```
Загружаем данные из базы данных в pandas DataFrame.

## Проверка наличия данных
```python
if df.empty:
    logging.error("Данные не получены из базы данных. Пожалуйста, проверьте ваш запрос и подключение к базе данных.")
    sys.exit(1)
```
Если данные не получены, выводим сообщение об ошибке и завершаем программу.

## Предварительный анализ данных
```python
print(f"Количество полученных строк: {len(df)}")
print(df.head())
```
Выводим количество полученных строк и первые несколько записей для проверки.

## Обработка столбца с датой
```python
df['ДАТА'] = pd.to_datetime(df['ДАТА'], errors='coerce')

if df['ДАТА'].isnull().any():
    logging.warning("Есть некорректные даты в столбце 'ДАТА'. Проверьте данные и формат даты.")
    print(df[df['ДАТА'].isnull()])
    sys.exit(1)
```
Преобразуем столбец 'ДАТА' в формат datetime и проверяем на наличие некорректных дат.

## Коррекция данных по часам
```python
df.loc[df['R_HOUR'] == 24, 'R_HOUR'] = 0
df.loc[df['R_HOUR'] == 0, 'ДАТА'] = df['ДАТА'] + pd.Timedelta(days=1)
```
Корректируем значения часа, если они равны 24, и сдвигаем дату на следующий день при необходимости.

## Создание столбца 'ds' и добавление дня недели
```python
df['ds'] = pd.to_datetime(df['ДАТА'].dt.strftime('%Y-%m-%d') + ' ' + df['R_HOUR'].astype(str) + ':00:00')
df['weekday'] = df['ds'].dt.weekday
```
Создаем столбец с полной датой и временем, а также определяем день недели.

## Агрегация данных и переиндексация
```python
df = df.groupby('ds', as_index=False).agg({'КОЛИЧЕСТВО': 'sum', 'weekday': 'first'})

start_date = df['ds'].min().normalize()
end_date = df['ds'].max().normalize() + pd.Timedelta(days=1)
complete_index = pd.date_range(start=start_date, end=end_date, freq='h')

df = df.set_index('ds').reindex(complete_index).reset_index()
df.rename(columns={'index': 'ds'}, inplace=True)
df['weekday'] = df['weekday'].fillna(df['ds'].dt.weekday)
```
Агрегируем данные по дате и часу, создаем полный диапазон дат и заполняем пропуски.

## Обработка пропущенных значений в 'КОЛИЧЕСТВО'
```python
if df['КОЛИЧЕСТВО'].isnull().any():
    logging.warning("Есть пустые значения в столбце 'КОЛИЧЕСТВО'. Заполняем средним значением соседних часов.")
    df.set_index('ds', inplace=True)
    df['КОЛИЧЕСТВО'] = df['КОЛИЧЕСТВО'].interpolate(method='time')
    df['КОЛИЧЕСТВО'] = df['КОЛИЧЕСТВО'].fillna(method='bfill').fillna(method='ffill')
    df.reset_index(inplace=True)
```
Заполняем пропущенные значения в количестве с помощью интерполяции и ближайших известных значений.

## Подготовка данных для обучения и тестирования
```python
historical_df = df[df['ds'] < '2024-10-22']
actual_df = df[(df['ds'] >= '2024-10-20') & (df['ds'] <= '2024-10-22')]
```
Разделяем данные на исторические для обучения модели и фактические для оценки прогноза.

## Функция для поиска похожих дней
```python
def find_similar_days(target_weekday, target_hour, data, window_size=10):
    similar_days = data[
        (data['weekday'] == target_weekday) & 
        (data['ds'].dt.hour == target_hour)
    ]
    if not similar_days.empty:
        rolling_mean = similar_days['КОЛИЧЕСТВО'].rolling(window=window_size, min_periods=1).mean().iloc[-1]
    else:
        rolling_mean = data['КОЛИЧЕСТВО'].mean()
    return rolling_mean
```
Определяем функцию для поиска аналогичных дней и расчета скользящего среднего потребления.

## Настройка и обучение модели Prophet
```python
prophet_df = historical_df[['ds', 'КОЛИЧЕСТВО']].rename(columns={'КОЛИЧЕСТВО': 'y'})
prophet_df = prophet_df[prophet_df['y'] >= 0]

model = Prophet(
    seasonality_mode='additive',
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=True,
    changepoint_prior_scale=0.05,
    seasonality_prior_scale=10.0
)

model.add_seasonality(name='weekly', period=7, fourier_order=3)
model.add_seasonality(name='daily', period=1, fourier_order=5)
model.add_seasonality(name='hourly', period=24, fourier_order=15)

model.fit(prophet_df)
```
Подготавливаем данные для модели, настраиваем сезонности и обучаем модель Prophet.

## Прогнозирование будущего потребления
```python
future = pd.DataFrame({'ds': forecast_dates})

forecast = model.predict(future)

forecast = forecast[['ds', 'yhat']]
forecast['yhat'] = forecast['yhat'].clip(lower=0)
```
Создаем прогноз потребления на будущие даты.

## Комбинирование прогноза с скользящим средним
```python
forecast['weekday'] = forecast['ds'].dt.weekday
forecast['hour'] = forecast['ds'].dt.hour

forecast['rolling_mean'] = forecast.apply(
    lambda row: find_similar_days(row['weekday'], row['hour'], historical_df), axis=1
)

forecast['combined_forecast'] = (0.3 * forecast['rolling_mean']) + (0.7 * forecast['yhat'])
forecast['combined_forecast'] = forecast['combined_forecast'].clip(lower=0)
```
Комбинируем прогноз модели с скользящим средним для повышения точности.

## Симуляция уровня топлива и определение даты достижения мертвого остатка
```python
initial_volume = 2804
forecast_df['DATE'] = forecast_df['ds'].dt.date

forecast_df['уровнемер'] = initial_volume - forecast_df['yhat'].cumsum()
dead_stock = 1565
```
Рассчитываем остаток топлива с учетом прогнозируемого потребления и определяем, когда уровень достигнет мертвого остатка.

## Проверка достижения мертвого остатка
```python
below_dead_stock = forecast_df[forecast_df['уровнемер'] <= dead_stock]

if not below_dead_stock.empty:
    first_date_reach_dead_stock = below_dead_stock.iloc[0]
    print(f"Мертвый остаток будет достигнут к: {first_date_reach_dead_stock['ds'].date()} в {first_date_reach_dead_stock['ds'].time()}")
else:
    print("Мертвый остаток не будет достигнут в пределах заданного периода.")
```
Выводим дату и время, когда будет достигнут мертвый остаток топлива.

## Расчет точности прогноза
```python
def calculate_simple_accuracy(actual, forecast):
    if pd.notnull(actual) and pd.notnull(forecast) and max(actual, forecast) != 0:
        return (min(actual, forecast) / max(actual, forecast)) * 100
    else:
        return None

merged_df['accuracy'] = merged_df.apply(lambda row: calculate_simple_accuracy(row['КОЛИЧЕСТВО'], row['yhat']), axis=1)

forecast_days_df = merged_df[(merged_df.index >= '2024-10-21') & (merged_df.index < '2024-10-22')]

daily_accuracy = forecast_days_df['accuracy'].mean()
print(f"Средняя точность прогноза на 21-22 октября 2024 года: {daily_accuracy:.2f}%")
```
Рассчитываем точность прогноза путем сравнения прогнозируемых и фактических значений.

## Сохранение результатов в CSV
```python
comparison_df = merged_df[['yhat', 'КОЛИЧЕСТВО', 'accuracy']]
comparison_df.to_csv('forecast_dates.csv')

print("CSV файл с прогнозом, фактическими данными и точностью создан: forecast_dates.csv")
```
Сохраняем результаты в файл **forecast_dates.csv** для дальнейшего анализа.

# Краткое резюме
Скрипт выполняет следующие основные действия:

- **Сбор данных**: Получает исторические данные по расходу топлива с выбранной АЗС и по выбранному виду топлива.

- **Обработка данных**: Предобрабатывает данные для устранения пропусков и аномалий.

- **Прогнозирование**: Использует модель Prophet для прогнозирования будущего потребления топлива.

- **Анализ уровня топлива**: Определяет, когда уровень топлива достигнет мертвого остатка.

- **Оценка точности**: Рассчитывает точность прогноза, сравнивая его с фактическими данными.

- **Сохранение результатов**: Сохраняет прогнозы и фактические данные в CSV-файл для дальнейшего использования.


