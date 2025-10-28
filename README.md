# Data_engineering_project
## О датасете
**Ссылка на датасет:** [Google Диск](https://drive.google.com/file/d/1zmNAMYzerjdrCZs51MmoWiHfvog-xdi3/view?usp=sharing)

**Информация о датасете:** [Kaggle](https://www.kaggle.com/datasets/akashnath29/lung-cancer-dataset/data)

Рак лёгких остаётся одной из самых распространённых и смертельных форм рака во всём мире, создавая серьёзные трудности для ранней диагностики и эффективного лечения. Этот набор данных представляет собой информацию о 20000 пациентах с раком лёгких и без, о поле возрасте и клинической картине заболевания.


## Загрузка виртуального окружения

Для загрузки рабочего окружения для работы с датасетом используйте файл environment.yml в репозитории и команду в терминале: 

```
conda env create -f environment.yml
```
## Загрузка датасета с Google Диска

Используйте скрипт data_loader.py и команду:

```
python data_loader.py
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
Приведённые типы данных в датафрейме:

<p align="center">
<img width="459" height="464" alt="image" src="https://github.com/user-attachments/assets/e5f03e63-8c72-4395-9714-8b96ec5c0004" />
</p>

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
python write_to_db.py
```
## Проведение EDA

Исследовательский анализ данных проведён в файле EDA.ipynb (находится в папке notebooks). Также в EDA.ipynb приведена визуализация для данного датасета, добавлена интерактивная визуализация с помощью poetry, интерактивные графики нахлдятся в виде html-файлов в папке notebooks.

Ссылка на рендер ноутбука с проведением EDA "EDA.ipynb": 
[Рендер](https://nbviewer.org/github/Therainbowmushroom/Data_engineering_project/blob/main/notebooks/EDA.ipynb)




