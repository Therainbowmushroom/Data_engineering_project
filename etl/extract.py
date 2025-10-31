import os
import pandas as pd
import pandera.pandas as pa
from dataclasses import dataclass
from dotenv import load_dotenv
from pandera import Column, Check


@dataclass
class Config:
    """Класс конфигурации для хранения всех параметров проекта"""
    file_id: str = "1zmNAMYzerjdrCZs51MmoWiHfvog-xdi3"
    raw_csv_path: str = './data/raw/df_lung.csv'


def load_config() -> Config:
    """Загрузка конфигурации"""
    load_dotenv()
    return Config(
        file_id=os.getenv("FILE_ID", "1zmNAMYzerjdrCZs51MmoWiHfvog-xdi3"),
        raw_csv_path=os.getenv("RAW_CSV_PATH", './data/raw/df_lung.csv'))


config = load_config()


def check_writing_dataset(df):
    """Проверка загрузки датасета"""
    if len(df) == 10:
        return 'Датасет загружен успешно'
    else:
        return 'Ошибка загрузки датасета'


def load_dataset_from_google_drive_url(config: Config):
    """Загрузка данных с гугл-диска"""
    print("Идёт загрузка данных с Google Drive...")
    file_url = f"https://drive.google.com/uc?id={config.file_id}"
    df = pd.read_csv(file_url)
    df_check = df.head(10)
    result = check_writing_dataset(df_check)
    print(result)
    print(f'Первые 10 строк датасета \n {df.head(10)}')
    return df


def save_dataset_to_csv(df, config: Config):
    """Сохранение датасета в csv-файл"""
    print("Идёт сохранение данных...")
    df.to_csv(config.raw_csv_path, index=False)


raw_schema_1 = pa.DataFrameSchema({
    "AGE": Column(int, nullable=False),
    "GENDER": Column(str, Check.isin(["F", "M"]), nullable=False),
    "SMOKING": Column(int, Check.isin([1, 2]), nullable=False),
    "YELLOW_FINGERS": Column(int, Check.isin([1, 2]), nullable=False),
    "ANXIETY": Column(int, Check.isin([1, 2]), nullable=False),
    "PEER_PRESSURE": Column(int, Check.isin([1, 2]), nullable=False),
    "CHRONIC DISEASE": Column(int, Check.isin([1, 2]), nullable=False),
    "FATIGUE ": Column(int, Check.isin([1, 2]), nullable=False),
    "ALLERGY ": Column(int, Check.isin([1, 2]), nullable=False),
    "WHEEZING": Column(int, Check.isin([1, 2]), nullable=False),
    "ALCOHOL CONSUMING": Column(int, Check.isin([1, 2]), nullable=False),
    "COUGHING": Column(int, Check.isin([1, 2]), nullable=False),
    "SHORTNESS OF BREATH": Column(int, Check.isin([1, 2]), nullable=False),
    "SWALLOWING DIFFICULTY": Column(int, Check.isin([1, 2]), nullable=False),
    "CHEST PAIN": Column(int, Check.isin([1, 2]), nullable=False),
    "LUNG_CANCER": Column(str, Check.isin(["NO", "YES"]), nullable=False)
    })


def validate_dataset_1(df):
    """Валидирует датасет согласно схеме raw_schema"""
    print("Идёт валидация данных...")
    try:
        valid_df = raw_schema_1.validate(df)
        print("Данные прошли валидацию успешно!")
        duplicate_count = valid_df.duplicated().sum()
        if duplicate_count > 0:
            print(f"В данных обнаружено {duplicate_count} дубликатов. Они будут сохранены в сырых данных.")
        return valid_df
    except pa.errors.SchemaError as e:
        print(f"Ошибка валидации: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        return None
