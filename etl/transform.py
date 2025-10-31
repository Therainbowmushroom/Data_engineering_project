import os
import pandas as pd
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class Config:
    """Класс конфигурации для хранения всех параметров проекта"""
    processed_csv_path: str = './data/raw/df_lung_preprocess.csv'
    raw_csv_path: str = './data/raw/df_lung.csv'


def load_config() -> Config:
    """Загрузка конфигурации"""
    load_dotenv()
    return Config(
        processed_csv_path=os.getenv("PROCESSED_CSV_PATH", './data/raw/df_lung_preprocess.csv'),
        raw_csv_path=os.getenv("RAW_CSV_PATH", './data/raw/df_lung.csv'))


config = load_config()


def read_dataset_1(config: Config):
    """Чтение данных из csv-данных"""
    df_lung = pd.read_csv(config.raw_csv_path)
    if len(df_lung) > 0:
        print('Данные успешно прочитаны')
    else:
        print('Ошибка чтения данных')
    print(df_lung.info())
    return df_lung


def rename_columns(df_lung):
    """Меняет названия колонок: убирает пробелы, заменяет их на _ и понижает регистр"""
    columns = df_lung.columns
    dict_rename = {}
    for col in columns:
        dict_rename[col] = col.lower().rstrip().replace(' ', '_')
    df_lung.rename(columns=dict_rename, inplace=True)
    return df_lung


def check_remove_duplicates(count, length, length_after):
    """Проверяет наличие дубликатов после их удаления"""
    if count == 0 and length == length_after:
        return 'Дубликатов нет'
    elif count != 0 and length != length_after:
        return f'Успешно удалено {count} дубликатов'
    else:
        return 'Ошибка удаления дубликатов'


def remove_duplicates(df_lung):
    """Удаляет дубликаты из датасета"""
    duplicate_count = df_lung.duplicated().sum()
    print(f"Найдено дубликатов: {duplicate_count}")
    length_df = len(df_lung)
    df_lung.drop_duplicates(inplace=True)
    length_df_after = len(df_lung)
    print(check_remove_duplicates(duplicate_count, length_df, length_df_after))
    return df_lung


class LungDataPreprocessor:
    """Класс для предобработки данных о раке легких"""
    
    def __init__(self):
        self.binary_columns = [
            'smoking', 'yellow_fingers', 'anxiety', 'peer_pressure', 
            'chronic_disease', 'fatigue', 'allergy', 'wheezing', 
            'alcohol_consuming', 'coughing', 'shortness_of_breath',
            'swallowing_difficulty', 'chest_pain'
        ]
    
    def transform_binary_columns(self, df):
        """Преобразование бинарных колонок"""
        print('Идёт приведения типов бинарных колонок к типу bool')
        df_transformed = df.copy()
        for column in self.binary_columns:
            df_transformed[column] = df_transformed[column].map({2: True, 1: False}).astype(bool)
        return df_transformed
    
    def transform_target_column(self, df):
        """Преобразование целевой переменной"""
        print('Идёт приведение типа колонки lung_cancer к типу bool')
        df_transformed = df.copy()
        df_transformed['lung_cancer'] = df_transformed['lung_cancer'].map(
            {'YES': True, 'NO': False}
        ).astype(bool)
        return df_transformed
    
    def add_metadata_columns(self, df):
        """Добавление служебных колонок"""
        print('Идёт замена колонки gender на колонку is_female')
        df_transformed = df.copy()
        df_transformed.insert(0, 'id', range(1, len(df_transformed) + 1))
        df_transformed['is_female'] = df_transformed['gender'].map({'F': True, 'M': False}).astype(bool)
        df_transformed = df_transformed.drop('gender', axis=1)
        return df_transformed
    
    def preprocess(self, df):
        """Полная предобработка данных"""
        df_processed = self.transform_binary_columns(df)
        df_processed = self.transform_target_column(df_processed)
        df_processed = self.add_metadata_columns(df_processed)
        return df_processed


def save_dataset(df, config: Config):
    """Сохранение предобработанного датасета"""
    df.to_csv(config.processed_csv_path, index=False)
