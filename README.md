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


