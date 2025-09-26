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
df_lung.columns

df_lung.isnull().sum()  # пропусков в столбцах нет
print(df_lung.duplicated().sum())  # полных дубликатов в датасете 429

df_lung.drop_duplicates(inplace=True)  # убрали дубликаты
df_lung.shape[0]  # количество строк
df_lung.info()

columns = df_lung.columns
for column in columns:
    print(df_lung[column].value_counts())

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
df_lung.info()

# Замена значений в столбце lung_cancer с YES и NO на True и False. 
# И замена значений в gender с M и F на 1 и 0 соотвественно.

df_lung['lung_cancer'] = df_lung['lung_cancer'].map(
    {'YES': True, 'NO': False}
    ).astype(bool)
df_lung['gender'] = df_lung['gender'].map({'M': 1, 'F': 0}).astype('category')
df_lung.info()

df_lung.to_parquet('lung_cancer_dataset_pyarrow.parquet', engine='pyarrow')

table = pa.read_table('lung_cancer_dataset_pyarrow.parquet')
print(table)