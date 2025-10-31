import os
import pandas as pd
import pandera.pandas as pa
from dataclasses import dataclass
from dotenv import load_dotenv
from pandera import Column, Check
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import Session


@dataclass
class Config:
    """Класс конфигурации для хранения всех параметров проекта"""
    db_user: str
    db_password: str
    db_url: str
    db_port: int
    db_name: str
    tb_name: str
    processed_csv_path: str = './data/raw/df_lung_preprocess.csv'
    raw_csv_path: str = './data/raw/df_lung.csv'
    parquet_path: str = './data/processed/df_lung_preprocess.parquet'


def load_config() -> Config:
    """Загрузка конфигурации"""
    load_dotenv()
    return Config(
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_url=os.getenv("DB_URL"),
        db_port=os.getenv("DB_PORT"),
        db_name=os.getenv("DB_NAME"),
        tb_name=os.getenv("TB_NAME"),
        processed_csv_path=os.getenv("PROCESSED_CSV_PATH", './data/raw/df_lung_preprocess.csv'),
        parquet_path=os.getenv("PARQUET_PATH", './data/processed/df_lung_preprocess.parquet'))


config = load_config()


def read_dataset_2(config: Config):
    """Читает датасет из файла csv"""
    print('Идёт чтение датасета...')
    df = pd.read_csv(config.processed_csv_path)
    return df


raw_schema_2 = pa.DataFrameSchema({
    "age": Column(int, nullable=False),
    "id": Column(int, nullable=False),
    "is_female": Column(bool, Check.isin([True, False]), nullable=False),
    "smoking": Column(bool, Check.isin([True, False]), nullable=False),
    "yellow_fingers": Column(bool, Check.isin([True, False]), nullable=False),
    "anxiety": Column(bool, Check.isin([True, False]), nullable=False),
    "peer_pressure": Column(bool, Check.isin([True, False]), nullable=False),
    "chronic_disease": Column(bool, Check.isin([True, False]), nullable=False),
    "fatigue": Column(bool, Check.isin([True, False]), nullable=False),
    "allergy": Column(bool, Check.isin([True, False]), nullable=False),
    "wheezing": Column(bool, Check.isin([True, False]), nullable=False),
    "alcohol_consuming": Column(bool, Check.isin([True, False]), nullable=False),
    "coughing": Column(bool, Check.isin([True, False]), nullable=False),
    "shortness_of_breath": Column(bool, Check.isin([True, False]), nullable=False),
    "swallowing_difficulty": Column(bool, Check.isin([True, False]), nullable=False),
    "chest_pain": Column(bool, Check.isin([True, False]), nullable=False),
    "lung_cancer": Column(bool, Check.isin([True, False]), nullable=False)
    }, checks=[Check(lambda df: df.duplicated().sum() == 0,
                     name="no_duplicate_rows", error="Обнаружены дубликаты строк в данных"
    )])


def validate_dataset_2(df):
    """Проводит валидацию данных согласно схеме raw_schema"""
    try:
        valid_df = raw_schema_2.validate(df)
        print("Данные прошли валидацию успешно!")
        return valid_df
    except pa.errors.SchemaError as e:
        print(f"Ошибка валидации: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        return None


def write_parquet(df, config: Config):
    """Записывает отвалидированный датасет в файл parquet"""
    df.to_parquet(config.parquet_path, engine='pyarrow')
    print(f'Данные сохранены в файл {config.parquet_path}')


def check_account_details():
    """Проверка наличия всех необходимых учётных данных"""
    required_vars = ["DB_USER", "DB_PASSWORD", "DB_URL", "DB_PORT", "DB_NAME", "TB_NAME"]
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    if missing_vars:
        print(f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
        print("Убедитесь, что файл .env существует и содержит все необходимые переменные:")
        for var in missing_vars:
            print(f"   - {var}")
        return False
    print("Все необходимые учётные данные присутствуют")
    return True


def setup_database_connection(config: Config):
    """Настройка подключения к базе данных"""
    if not check_account_details():
        print("Не удалось создать подключение к БД: отсутствуют учётные данные")
    try:
        engine = create_engine(f'postgresql+psycopg2://{config.db_user}:{config.db_password}@{config.db_url}:{config.db_port}/{config.db_name}')
        print(f'Движок БД создан и подключение успешно: {engine}')
        return engine
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        raise


def read_parquet(config: Config):
    """Чтение датасета из файла parquet"""
    df = pd.read_parquet(config.parquet_path)
    print('Данные загружены из Parquet файла')
    print('Типы данных в загруженном датасете:')
    print(df.dtypes)
    df_for_upload = df.head(70)
    return df_for_upload


def upload_data_with_primary_key(engine, df, config: Config):
    """Загрузка данных в БД с установкой первичного ключа"""
    try:
        # Проверяем, есть ли колонка id в данных
        if 'id' not in df.columns:
            print("В данных отсутствует колонка 'id' для создания первичного ключа")
            return False
        df.to_sql(
            name=config.tb_name,
            con=engine,
            schema='public',
            if_exists='replace',
            index=False
        )
        print(f'Данные загружены в таблицу {config.tb_name}')
        session = Session(bind=engine)
        with session as s:
            s.execute(text(f'ALTER TABLE {config.tb_name} ADD PRIMARY KEY (id);'))
            s.commit()
        print('Колонка id установлена как первичный ключ')
        return True
    except Exception as e:
        print(f"Ошибка при загрузке данных в БД: {e}")
        return False


def check_data_upload(engine, config: Config, expected_count=70):
    """Проверка корректности загрузки данных"""
    try:
        # Проверяем существование таблицы
        inspector = inspect(engine)
        if config.tb_name not in inspector.get_table_names(schema='public'):
            print(f"Таблица {config.tb_name} не существует")
            return None, None, None
        session = Session(bind=engine)
        with session as s:
            # Проверяем общее количество строк
            count_result = s.execute(text(f'SELECT COUNT(*) FROM {config.tb_name}'))
            actual_count = count_result.scalar()
            result = s.execute(text(f'SELECT * FROM {config.tb_name} LIMIT 10'))
            columns = result.keys()
            rows = result.fetchall()
            columns_info = inspector.get_columns(config.tb_name, schema='public')
            print(f"\nТипы данных в таблице {config.tb_name} в БД:")
            for col in columns_info:
                print(f"- {col['name']}: {col['type']}")
            if actual_count == expected_count:
                print("Таблица успешно записана в БД")
            else:
                print(f"Ошибка записи. Ожидалось {expected_count} строк, получено {actual_count}")
            print("Первые 10 строк таблицы:")
            for row in rows:
                print(row)
            print(f"Названия колонок: {', '.join(columns)}")
            return actual_count, columns, rows
    except Exception as e:
        print(f'Ошибка при проверке данных: {e}')
        return None, None, None