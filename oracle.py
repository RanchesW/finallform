import pandas as pd
import matplotlib.pyplot as plt
import logging
from prophet import Prophet
from sqlalchemy import create_engine
import cx_Oracle
import sys

# Инициализация Oracle Instant Client
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\aalik\Documents\instantclient_23_4")

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ваши данные для подключения
username = 'Alikhan'  # Оставьте ваш логин
password = 'Ваш пароль'  # Введите ваш пароль
host = '10.10.120.96'
port = 1521
service_name = 'ORCL'

# Создание строки подключения
dsn = cx_Oracle.makedsn(host, port, service_name=service_name)

# Создание движка SQLAlchemy
engine = create_engine(f'oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service_name}')

# Создание словаря для отображения названия топлива на GASNUM
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

# Получение номера АЗС от пользователя
object_code = input("Введите номер АЗС (OBJECTCODE): ")  # Например, 'Z313'

# Вывод доступных видов топлива
print("Доступные виды топлива:")
for fuel_name in fuel_mapping.keys():
    print(f"- {fuel_name}")

# Получение названия топлива от пользователя
fuel_name = input("Введите название топлива из списка выше: ")  # Например, 'Аи-92'

# Проверка наличия введенного топлива в словаре
if fuel_name not in fuel_mapping:
    print("Введено некорректное название топлива. Пожалуйста, выберите из списка.")
    sys.exit(1)

# Получение соответствующего GASNUM
gasnum = fuel_mapping[fuel_name]

# SQL-запрос с фильтрацией по OBJECTCODE, GASNUM и TANK
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

# Загрузка данных из базы данных с параметрами
try:
    df = pd.read_sql(query, con=engine, params={'object_code': object_code, 'gasnum': gasnum})
except Exception as e:
    logging.error(f"Ошибка при чтении данных из базы данных: {e}")
    sys.exit(1)

# Проверка, что данные загружены
if df.empty:
    logging.error("Данные не получены из базы данных. Пожалуйста, проверьте ваш запрос и подключение к базе данных.")
    sys.exit(1)

print(f"Количество полученных строк: {len(df)}")
print(df.head())

# Преобразование столбца 'ДАТА' в формат datetime
df['ДАТА'] = pd.to_datetime(df['ДАТА'], errors='coerce')

# Проверка на некорректные даты
if df['ДАТА'].isnull().any():
    logging.warning("Есть некорректные даты в столбце 'ДАТА'. Проверьте данные и формат даты.")
    print(df[df['ДАТА'].isnull()])
    sys.exit(1)

# Корректировка столбца 'R_HOUR' при значении 24
df.loc[df['R_HOUR'] == 24, 'R_HOUR'] = 0
df.loc[df['R_HOUR'] == 0, 'ДАТА'] = df['ДАТА'] + pd.Timedelta(days=1)

# Создание столбца 'ds' с комбинацией даты и часа
df['ds'] = pd.to_datetime(df['ДАТА'].dt.strftime('%Y-%m-%d') + ' ' + df['R_HOUR'].astype(str) + ':00:00')

# Добавление столбца 'weekday' для определения дня недели
df['weekday'] = df['ds'].dt.weekday

# Удаление дубликатов по 'ds' и агрегация 'КОЛИЧЕСТВО'
df = df.groupby('ds', as_index=False).agg({'КОЛИЧЕСТВО': 'sum', 'weekday': 'first'})

# Генерация полного диапазона дат и часов
start_date = df['ds'].min().normalize()
end_date = df['ds'].max().normalize() + pd.Timedelta(days=1)
complete_index = pd.date_range(start=start_date, end=end_date, freq='h')

# Переиндексация и заполнение пропусков
df = df.set_index('ds').reindex(complete_index).reset_index()
df.rename(columns={'index': 'ds'}, inplace=True)

# Заполнение пропущенных значений в 'weekday'
df['weekday'] = df['weekday'].fillna(df['ds'].dt.weekday)

# Проверка на пропущенные значения в 'КОЛИЧЕСТВО' и заполнение средним значением соседних часов
if df['КОЛИЧЕСТВО'].isnull().any():
    logging.warning("Есть пустые значения в столбце 'КОЛИЧЕСТВО'. Заполняем средним значением соседних часов.")
    # Устанавливаем 'ds' как индекс
    df.set_index('ds', inplace=True)
    # Интерполяция по времени
    df['КОЛИЧЕСТВО'] = df['КОЛИЧЕСТВО'].interpolate(method='time')
    # Заполнение оставшихся пропусков ближайшими значениями
    df['КОЛИЧЕСТВО'] = df['КОЛИЧЕСТВО'].fillna(method='bfill').fillna(method='ffill')
    # Сбрасываем индекс
    df.reset_index(inplace=True)

# Подготовка исторических данных до определенной даты
historical_df = df[df['ds'] < '2024-10-22']

# Подготовка фактических данных с определенной даты
actual_df = df[(df['ds'] >= '2024-10-20') & (df['ds'] <= '2024-10-22')]

# Функция для поиска похожих дней и расчета скользящего среднего
def find_similar_days(target_weekday, target_hour, data, window_size=10):
    similar_days = data[
        (data['weekday'] == target_weekday) & 
        (data['ds'].dt.hour == target_hour)
    ]
    if not similar_days.empty:
        rolling_mean = similar_days['КОЛИЧЕСТВО'].rolling(window=window_size, min_periods=1).mean().iloc[-1]
    else:
        rolling_mean = data['КОЛИЧЕСТВО'].mean()  # Используем общее среднее, если нет похожих дней
    return rolling_mean

# Определение дат для прогноза
forecast_start_date = '2024-10-21 07:00:00'
forecast_end_date = '2024-10-21 23:59:59'
forecast_dates = pd.date_range(start=forecast_start_date, end=forecast_end_date, freq='h')

# Подготовка данных для модели Prophet
prophet_df = historical_df[['ds', 'КОЛИЧЕСТВО']].rename(columns={'КОЛИЧЕСТВО': 'y'})
prophet_df = prophet_df[prophet_df['y'] >= 0]  # Удаляем отрицательные значения

# Настройка модели Prophet
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

# Обучение модели
model.fit(prophet_df)

# Создание future DataFrame с нужными датами
future = pd.DataFrame({'ds': forecast_dates})

# Прогнозирование с помощью модели Prophet
forecast = model.predict(future)

# Подготовка прогноза
forecast = forecast[['ds', 'yhat']]
forecast['yhat'] = forecast['yhat'].clip(lower=0)

# Добавление 'weekday' и 'hour' для будущих дат
forecast['weekday'] = forecast['ds'].dt.weekday
forecast['hour'] = forecast['ds'].dt.hour

# Применение функции для расчета скользящего среднего
forecast['rolling_mean'] = forecast.apply(
    lambda row: find_similar_days(row['weekday'], row['hour'], historical_df), axis=1
)

# Комбинирование прогноза Prophet со скользящим средним
forecast['combined_forecast'] = (0.3 * forecast['rolling_mean']) + (0.7 * forecast['yhat'])
forecast['combined_forecast'] = forecast['combined_forecast'].clip(lower=0)

# Получение списка прогнозных значений
forecast_values = forecast['combined_forecast'].tolist()

# Создание DataFrame для гибридного прогноза
forecast_df = pd.DataFrame({'ds': forecast_dates, 'yhat': forecast_values})

# Начальный уровень объема
initial_volume = 2804
forecast_df['DATE'] = forecast_df['ds'].dt.date

# Прогноз уровня и проверка на достижение мертвого остатка
forecast_df['уровнемер'] = initial_volume - forecast_df['yhat'].cumsum()
dead_stock = 1565
max_iterations = 1000
iteration_count = 0

while forecast_df['уровнемер'].iloc[-1] > dead_stock:
    if iteration_count >= max_iterations:
        logging.warning("Достигнуто максимальное количество итераций. Прерываем цикл, чтобы избежать бесконечного выполнения.")
        break

    next_date = forecast_df['ds'].iloc[-1] + pd.Timedelta(hours=1)
    target_weekday = next_date.weekday()
    target_hour = next_date.hour
    next_volume_consumption = find_similar_days(target_weekday, target_hour, historical_df)
    next_volume = forecast_df['уровнемер'].iloc[-1] - next_volume_consumption

    next_forecast = pd.DataFrame({
        'ds': [next_date],
        'yhat': [next_volume_consumption],
        'уровнемер': [next_volume],
        'DATE': [next_date.date()]
    })

    forecast_df = pd.concat([forecast_df, next_forecast], ignore_index=True)
    iteration_count += 1

# Проверка на достижение мертвого остатка
below_dead_stock = forecast_df[forecast_df['уровнемер'] <= dead_stock]

if not below_dead_stock.empty:
    first_date_reach_dead_stock = below_dead_stock.iloc[0]
    print(f"Мертвый остаток будет достигнут к: {first_date_reach_dead_stock['ds'].date()} в {first_date_reach_dead_stock['ds'].time()}")
else:
    print("Мертвый остаток не будет достигнут в пределах заданного периода.")

# Установка индекса 'ds' для объединения
forecast_df = forecast_df.set_index('ds')
actual_df = actual_df.set_index('ds')

# Объединение фактических данных с прогнозом для расчета точности
merged_df = forecast_df.join(actual_df[['КОЛИЧЕСТВО']], how='left', rsuffix='_actual')

# Функция для расчета точности
def calculate_simple_accuracy(actual, forecast):
    if pd.notnull(actual) and pd.notnull(forecast) and max(actual, forecast) != 0:
        return (min(actual, forecast) / max(actual, forecast)) * 100
    else:
        return None

# Расчет точности для всех данных
merged_df['accuracy'] = merged_df.apply(lambda row: calculate_simple_accuracy(row['КОЛИЧЕСТВО'], row['yhat']), axis=1)

# Фильтрация данных для расчета точности на определенные даты
forecast_days_df = merged_df[(merged_df.index >= '2024-10-21') & (merged_df.index < '2024-10-22')]

# Расчет средней точности
daily_accuracy = forecast_days_df['accuracy'].mean()
print(f"Средняя точность прогноза на 21-22 октября 2024 года: {daily_accuracy:.2f}%")

# Сохранение прогноза и фактических данных в CSV для сравнения
comparison_df = merged_df[['yhat', 'КОЛИЧЕСТВО', 'accuracy']]
comparison_df.to_csv('forecast_dates.csv')

print("CSV файл с прогнозом, фактическими данными и точностью создан: forecast_dates.csv")
