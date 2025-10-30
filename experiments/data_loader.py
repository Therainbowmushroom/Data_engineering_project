import pandas as pd
import pyarrow.parquet as pa

file_id = "1zmNAMYzerjdrCZs51MmoWiHfvog-xdi3"  # ID файла на Google Drive
file_url = f"https://drive.google.com/uc?id={file_id}"

df_lung = pd.read_csv(file_url)

print(df_lung.head(10))

columns = df_lung.columns
dict_rename = {}
for col in columns:
    dict_rename[col] = col.lower().rstrip().replace(' ', '_')
df_lung.rename(columns=dict_rename, inplace=True)
print(df_lung.columns)

print(df_lung.isnull().sum())  # пропусков в столбцах нет
print(df_lung.duplicated().sum())  # полных дубликатов в датасете 429

df_lung.drop_duplicates(inplace=True)  # убрали дубликаты
print(df_lung.shape[0])  # количество строк

columns = df_lung.columns

# Замена значений в столбцах: 'smoking', 'yellow_fingers', 'anxiety',
# 'peer_pressure', 'chronic_disease', 'fatigue', 'allergy', 'wheezing', 
# 'alcohol_consuming', 'coughing', 'shortness_of_breath',
# 'swallowing_difficulty', 'chest_pain' 
# - на значения True и False и приведение значений к типу bool.

column_age_lung = [
    'smoking', 'yellow_fingers', 'anxiety', 'peer_pressure', 
    'chronic_disease', 'fatigue', 'allergy', 'wheezing', 
    'alcohol_consuming', 'coughing', 'shortness_of_breath',
    'swallowing_difficulty', 'chest_pain'
    ]
for column in column_age_lung:    
    df_lung[column] = df_lung[column].map({2: True, 1: False}).astype(bool)
print(df_lung.info())

# Замена значений в столбце lung_cancer с YES и NO на True и False. 
# И замена значений в gender с M и F на 1 и 0 соотвественно.

df_lung['lung_cancer'] = df_lung['lung_cancer'].map(
    {'YES': True, 'NO': False}
    ).astype(bool)
df_lung.insert(0, 'id', range(1, len(df_lung) + 1))
df_lung['is_female'] = [True if gender == 'F' else False for gender in df_lung['gender']]
df_lung = df_lung.drop('gender', axis=1)

print(df_lung.info())
print(df_lung.head(10))

df_lung.to_parquet('lung_cancer_dataset_pyarrow.parquet', engine='pyarrow')

table = pa.read_table('lung_cancer_dataset_pyarrow.parquet')
print(table)

