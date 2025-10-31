# Data_engineering_project
## О датасете
**Ссылка на датасет:** [Google Диск](https://drive.google.com/file/d/1zmNAMYzerjdrCZs51MmoWiHfvog-xdi3/view?usp=sharing)

**Информация о датасете:** [Kaggle](https://www.kaggle.com/datasets/akashnath29/lung-cancer-dataset/data)

Рак лёгких остаётся одной из самых распространённых и смертельных форм рака во всём мире, создавая серьёзные трудности для ранней диагностики и эффективного лечения. Этот набор данных представляет собой информацию о 20000 пациентах с раком лёгких и без, о поле возрасте и клинической картине заболевания.

<p align="center">
<img width="358" height="313" alt="image" src="https://github.com/user-attachments/assets/9820524b-b216-47d6-a8b7-0b84a2978d84" />
</p>


Атрибуты датасета:

| Название колонки | Описание | Тип данных | Значения |
|---------|----------|----------|----------|
| AGE | Возраст на момент постановки диагноза | int64 | 30-87 |
| GENDER | Пол пациента | object | F или M |
| SMOKING | Статус курения | int64 | 1 или 2 |
| YELLOW_FINGERS | Пожелтение пальцев | int64 | 1 или 2 |
| ANXIETY | Тревожность | int64 | 1 или 2 |
| PEER_PRESSURE | Давление со стороны сверстников | int64 | 1 или 2 |
| CHRONIC DISEASE | Наличие хронических заболеваний | int64 | 1 или 2 |
| FATIGUE | Переутомление | int64 | 1 или 2 |
| ALLERGY | Наличие аллергии | int64 | 1 или 2 |
| WHEEZING | Свистящий звук при дыхании | int64 | 1 или 2 |
| ALCOHOL CONSUMING | Употребление алкоголя | int64 | 1 или 2 |
| COUGHING | Кашель | int64 | 1 или 2 |
| SHORTNESS OF BREATH | Одышка | int64 | 1 или 2 |
| SWALLOWING DIFFICULTY | Затруднённое дыхание | int64 | 1 или 2 |
| CHEST PAIN | Боль в груди | int64 | 1 или 2 |
| LUNG_CANCER | Наличие рака лёгких | object | NO или YES |


## Загрузка виртуального окружения

Для загрузки рабочего окружения для работы с датасетом используйте файл environment.yml в репозитории и команду в терминале: 

```
conda env create -f environment.yml
```
## Загрузка датасета с Google Диска

Используйте скрипт data_loader.py и команду:

```
python ./experiments/data_loader.py
```

Результат команды: 

```
print(df_lung.head(10))
```
<p align="center">
<img width="1218" height="296" alt="image" src="https://github.com/user-attachments/assets/c47e4ff4-85cf-4264-9dd8-10d4b07ea481" />
</p>
Исходные типы данных в датафрейме:

<p align="center">
<img width="520" height="510" alt="image" src="https://github.com/user-attachments/assets/b3ace559-f1f3-43d4-b592-72746b3c38ab" />
</p>
<p align="center">
Занимаемая память: 2.5+ MB
</p>

Колонки, содержащие только два различных значения, были приведены к типу bool, колонка gender была дополнительно заменена на бинарный признак is_female с типом данных bool.
Также добавлена колонка id, содержащая значения типа int64.

Приведённые типы данных в датафрейме:

<p align="center">
<img width="460" height="421" alt="image" src="https://github.com/user-attachments/assets/85fb29b8-bee9-4db1-8f6d-d517a0152de9" />
</p>
<p align="center">
Занимаемая память: 745.4 KB
</p>
Датасет с приведёнными типами данных записывается в файл *.parquet

## Загрузка датасета в базу данных:
Для создания подключения использовался файл .env:

Пример файла:
```
DB_USER=имя_пользователя
DB_PASSWORD=пароль
DB_URL=url_базы_данных
DB_PORT=код_порта_базы_данных
DB_NAME=название_базы_данных
TB_NAME=название_таблицы
```
Используйте скрипт write_to_db.py и команду в строке:
```
python ./experiments/write_to_db.py
```
Загрузка осуществляется из файла *.parquet. В результате в базу данных записывается 70 строк датасета.

<p align="center">
<img width="964" height="226" alt="image" src="https://github.com/user-attachments/assets/d8ded631-db9e-4cc6-a7df-00a6b7d40ed4" />
Первые 10 строк таблицы в базе данных
</p>

## Проведение EDA

Исследовательский анализ данных проведён в файле EDA.ipynb (находится в папке notebooks). Также в EDA.ipynb приведена визуализация для данного датасета, добавлена интерактивная визуализация с помощью poetry, интерактивные графики находятся в виде html-файлов в папке notebooks (plot_lung_cancer.html, plot_fatigue.html, plot_chronic_disease.html), так как рендер ноутбука не поддерживает интерактивные графики.

Ссылка на рендер ноутбука с проведением EDA "EDA.ipynb": 
[Рендер](https://nbviewer.org/github/Therainbowmushroom/Data_engineering_project/blob/main/notebooks/EDA.ipynb)

## Проведение ETL
Структура модуля для проведения ETL:
```
Data_engineering_project/
│
├── etl/
│   ├── __init__.py
│   ├── extract.py     # Загрузка датасета из Google Drive и первичная валидация данных
│   ├── load.py        # Вторичная валидация данных, загрузка трансформированного датасета в *.parquet и в БД
│   ├── main.py        # Точка входа для запуска ETL-процесса
│   └── transform.py   # Приведение типов данных исходного датасета

```
**Описание компонентов модуля ETL**

```etl/extract.py``` - осуществляет загрузку датасета из Google Drive, проводит проверки на соответствие типов данных в колонках, наличие пропусков и дубликатов согласно схеме в файле, сохраняет датасет в файл ./data/raw/df_lung.csv.

```etl/transform.py``` - осуществляет загрузку датасета из файла ./data/raw/df_lung.csv, выполняет переименование названий колонок, приведение типов колонок, смена колонки gender на is_female и добавление колонки с id и сохранение предобработанного датасета в файл ./data/raw/df_lung_preprocess.csv

```etl/load.py``` - читает файл ./data/raw/df_lung_preprocess.csv, проводит проверки на типы данных, пропуски и дубликаты, записывает файл ./data/processed/df_lung_preprocess.parquet, и загружает первые 70 строк в БД, устанавливает как первичный ключ колонку id, проверяет запись датасета в таблицу выводом десяти первых строк и типов колонок.

```etl/main.py``` - осуществляет запуск всего процесса ETL, для успешного запуска необходим файл .env, в котором должны быть следующие поля:

```
FILE_ID=1zmNAMYzerjdrCZs51MmoWiHfvog-xdi3
RAW_CSV_PATH=./data/raw/df_lung.csv
PROCESSED_CSV_PATH=./data/raw/df_lung_preprocess.csv
PARQUET_PATH=./data/processed/df_lung_preprocess.parquet
DB_USER=имя_пользователя
DB_PASSWORD=пароль
DB_URL=url_базы_данных
DB_PORT=код_порта_базы_данных
DB_NAME=название_базы_данных
TB_NAME=название_таблицы
```

Запуск ETL-процесса осуществляется командой:

```
python etl/main.py
```

Вывод в терминале:
```
Идёт загрузка данных с Google Drive...
Датасет загружен успешно
Первые 10 строк датасета 
   GENDER  AGE  SMOKING  YELLOW_FINGERS  ANXIETY  ...  COUGHING  SHORTNESS OF BREATH  SWALLOWING DIFFICULTY  CHEST PAIN  LUNG_CANCER
0      M   69        2               1        1  ...         2                    2                      1           1          YES
1      M   71        2               2        1  ...         1                    2                      2           1          YES
2      M   61        2               1        1  ...         1                    2                      2           2           NO
3      M   55        2               2        1  ...         1                    2                      2           2          YES
4      F   56        2               1        1  ...         1                    2                      2           2          YES
5      F   53        2               2        2  ...         2                    2                      2           2           NO
6      F   54        2               2        2  ...         1                    2                      1           1          YES
7      M   69        1               2        1  ...         1                    2                      1           1          YES
8      F   63        2               2        1  ...         2                    2                      1           1          YES
9      M   60        1               1        2  ...         2                    2                      1           2          YES

[10 rows x 16 columns]
Идёт валидация данных...
Идёт валидация данных...
Данные прошли валидацию успешно!
В данных обнаружено 429 дубликатов. Они будут сохранены в сырых данных.
Идёт сохранение данных...
Идёт чтение датасета из файла ./data/raw/df_lung.csv...
Данные успешно прочитаны
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 20000 entries, 0 to 19999
Data columns (total 16 columns):
 #   Column                 Non-Null Count  Dtype
---  ------                 --------------  -----
 0   GENDER                 20000 non-null  object
 1   AGE                    20000 non-null  int64
 2   SMOKING                20000 non-null  int64
 3   YELLOW_FINGERS         20000 non-null  int64
 4   ANXIETY                20000 non-null  int64
 5   PEER_PRESSURE          20000 non-null  int64
 6   CHRONIC DISEASE        20000 non-null  int64
 7   FATIGUE                20000 non-null  int64
 8   ALLERGY                20000 non-null  int64
 9   WHEEZING               20000 non-null  int64
 10  ALCOHOL CONSUMING      20000 non-null  int64
 11  COUGHING               20000 non-null  int64
 12  SHORTNESS OF BREATH    20000 non-null  int64
 13  SWALLOWING DIFFICULTY  20000 non-null  int64
 14  CHEST PAIN             20000 non-null  int64
 15  LUNG_CANCER            20000 non-null  object
dtypes: int64(14), object(2)
memory usage: 2.4+ MB
None
Идёт приведение названий колонок к общему виду...
Идёт удаление дубликатов...
Найдено дубликатов: 429
Успешно удалено 429 дубликатов
Идёт приведение типов и значений
Идёт приведения типов бинарных колонок к типу bool
Идёт приведение типа колонки lung_cancer к типу bool
Идёт замена колонки gender на колонку is_female
Идёт сохранение датасета как ./data/raw/df_lung_preprocess.csv
Десять строк обработанного датасета:
   id  age  smoking  yellow_fingers  anxiety  peer_pressure  ...  coughing  shortness_of_breath  swallowing_difficulty  chest_pain  lung_cancer  is_female0   1   69     True           False    False           True  ...      True                 True                  False       False         True      False1   2   71     True            True    False          False  ...     False                 True                   True       False         True      False2   3   61     True           False    False           True  ...     False                 True                   True        True        False      False3   4   55     True            True    False           True  ...     False                 True                   True        True         True      False4   5   56     True           False    False          False  ...     False                 True                   True        True         True       True5   6   53     True            True     True           True  ...      True                 True                   True        True        False       True6   7   54     True            True     True           True  ...     False                 True                  False       False         True       True7   8   69    False            True    False           True  ...     False                 True                  False       False         True      False8   9   63     True            True    False          False  ...      True                 True                  False       False         True       True9  10   60    False           False     True           True  ...      True                 True                  False        True         True      False
[10 rows x 17 columns]
<class 'pandas.core.frame.DataFrame'>
Index: 19571 entries, 0 to 19999
Data columns (total 17 columns):
 #   Column                 Non-Null Count  Dtype
---  ------                 --------------  -----
 0   id                     19571 non-null  int64
 1   age                    19571 non-null  int64
 2   smoking                19571 non-null  bool
 3   yellow_fingers         19571 non-null  bool
 4   anxiety                19571 non-null  bool
 5   peer_pressure          19571 non-null  bool
 6   chronic_disease        19571 non-null  bool
 7   fatigue                19571 non-null  bool
 8   allergy                19571 non-null  bool
 9   wheezing               19571 non-null  bool
 10  alcohol_consuming      19571 non-null  bool
 11  coughing               19571 non-null  bool
 12  shortness_of_breath    19571 non-null  bool
 13  swallowing_difficulty  19571 non-null  bool
 14  chest_pain             19571 non-null  bool
 15  lung_cancer            19571 non-null  bool
 16  is_female              19571 non-null  bool
dtypes: bool(15), int64(2)
memory usage: 745.4 KB
None
Идёт чтение датасета...
Данные прошли валидацию успешно!
Данные сохранены в файл ./data/processed/df_lung_preprocess.parquet
Все необходимые учётные данные присутствуют
Движок БД создан и подключение успешно:
Данные загружены из Parquet файла
Типы данных в загруженном датасете:
id                       int64
age                      int64
smoking                   bool
yellow_fingers            bool
anxiety                   bool
peer_pressure             bool
chronic_disease           bool
fatigue                   bool
allergy                   bool
wheezing                  bool
alcohol_consuming         bool
coughing                  bool
shortness_of_breath       bool
swallowing_difficulty     bool
chest_pain                bool
lung_cancer               bool
is_female                 bool
dtype: object
Данные загружены в таблицу
Колонка id установлена как первичный ключ

Типы данных в таблице в БД:
- id: BIGINT
- age: BIGINT
- smoking: BOOLEAN
- yellow_fingers: BOOLEAN
- anxiety: BOOLEAN
- peer_pressure: BOOLEAN
- chronic_disease: BOOLEAN
- fatigue: BOOLEAN
- allergy: BOOLEAN
- wheezing: BOOLEAN
- alcohol_consuming: BOOLEAN
- coughing: BOOLEAN
- shortness_of_breath: BOOLEAN
- swallowing_difficulty: BOOLEAN
- chest_pain: BOOLEAN
- lung_cancer: BOOLEAN
- is_female: BOOLEAN
Таблица успешно записана в БД
Первые 10 строк таблицы:
(1, 69, True, False, False, True, False, True, False, False, True, True, True, False, False, True, False)
(2, 71, True, True, False, False, True, False, True, True, False, False, True, True, False, True, False)
(3, 61, True, False, False, True, True, False, True, True, False, False, True, True, True, False, False)
(4, 55, True, True, False, True, False, False, False, True, True, False, True, True, True, True, False)
(5, 56, True, False, False, False, False, True, True, True, True, False, True, True, True, True, True)
(6, 53, True, True, True, True, True, False, True, False, False, True, True, True, True, False, True)
(7, 54, True, True, True, True, False, True, False, True, True, False, True, False, False, True, True)
(8, 69, False, True, False, True, True, True, True, False, False, False, True, False, False, True, False)
(9, 63, True, True, False, False, True, False, False, True, True, True, True, False, False, True, True)
(10, 60, False, False, True, True, True, True, True, True, True, True, True, False, True, True, False)
Названия колонок: id, age, smoking, yellow_fingers, anxiety, peer_pressure, chronic_disease, fatigue, allergy, wheezing, alcohol_consuming, coughing, shortness_of_breath, swallowing_difficulty, chest_pain, lung_cancer, is_female
```

