import os
from dotenv import load_dotenv

from dataclasses import dataclass
from extract import (load_dataset_from_google_drive_url, validate_dataset_1, 
                     save_dataset_to_csv, raw_schema_1)
from transform import (read_dataset_1, rename_columns, remove_duplicates, 
                       save_dataset, LungDataPreprocessor)
from load import (read_dataset_2, validate_dataset_2, write_parquet, 
                  setup_database_connection, read_parquet, 
                  upload_data_with_primary_key, check_data_upload,
                  raw_schema_2)

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
    file_id: str = "1zmNAMYzerjdrCZs51MmoWiHfvog-xdi3"
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
        raw_csv_path=os.getenv("RAW_CSV_PATH", './data/raw/df_lung.csv'),
        file_id=os.getenv("FILE_ID", "1zmNAMYzerjdrCZs51MmoWiHfvog-xdi3"),
        parquet_path=os.getenv("PARQUET_PATH", './data/processed/df_lung_preprocess.parquet'))

config = load_config()

def etl_process():

    '''Загрузка и валидация необработанных (сырых) данных'''

    df_lung = load_dataset_from_google_drive_url(config)
    print("Идёт валидация данных...")
    validated_df = validate_dataset_1(df_lung)
    if validated_df is not None:
        save_dataset_to_csv(validated_df, config)
    else:
        print("Валидация не пройдена, но сохраняем исходные данные...")
        save_dataset_to_csv(df_lung, config)

    """Трансформация датасета (изменение названия колонок, приведение типов и тд)"""

    print(f'Идёт чтение датасета из файла {config}...')
    df = read_dataset_1(config)
    print('Идёт приведение названий колонок к общему виду...')
    df = rename_columns(df)
    print('Идёт удаление дубликатов...')
    df = remove_duplicates(df)
    print('Идёт приведение типов и значений')
    preprocessor = LungDataPreprocessor()
    df = preprocessor.preprocess(df)
    print(f'Идёт сохранение датасета как {config}')
    save_dataset(df, config)
    if df is not None:
        print('Десять строк обработанного датасета:')
        print(df.head(10))
        print(df.info())

    """Валидация предобработанных данных, запись их в файл parquet и загрузка в БД"""

    df_lung = read_dataset_2(config)
    df_lung = validate_dataset_2(df_lung)
    write_parquet(df_lung, config)
    engine = setup_database_connection(config)
    df_parq = read_parquet(config)
    success = upload_data_with_primary_key(engine, df_parq, config)
    if not success:
        print("Прерывание выполнения из-за ошибки загрузки данных")
    check_data_upload(engine, config, expected_count=70)


if __name__ == "__main__":
    etl_process()