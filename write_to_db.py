import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import Session
import pandas as pd

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_url = os.getenv("DB_URL")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_url}:{db_port}/{db_name}')
print(f'Движок доступен: {engine}')

df_lung_parq = pd.read_parquet('lung_cancer_dataset_pyarrow.parquet')
print('Сохранённые типы данных:')
print(df_lung_parq.dtypes)

df_for_upload = df_lung_parq.head(70)
df_for_upload.to_sql(name='nikolaeva',
                     con=engine,
                     schema='public',
                     if_exists='replace',
                     index=False)

inspector = inspect(engine)
columns_info = inspector.get_columns('nikolaeva', schema='public')
print("\nТипы данных в таблице 'nikolaeva' в БД:")
for col in columns_info:
    print(f"- {col['name']}: {col['type']}")

session = Session(bind=engine)
with session as s:
    example_total = s.execute(text('SELECT * FROM nikolaeva')).fetchall()
    result = s.execute(text('SELECT * FROM nikolaeva LIMIT 10'))
    len_example_total = s.execute(text('SELECT COUNT(*) FROM nikolaeva')).scalar()

    columns = result.keys()
    rows = result.fetchall()
    
    if len_example_total == 70:
        print(f'Таблица записалась успешно')
    else:
        print(f'Ошибка записи. Ожидалось 70 строк, получено {len_example_total}')
    print('Вывод таблицы (первые 10 строк):')
    for row in rows:
        print(row)
    print("Названия колонок:")
    print(' '.join(columns))
    