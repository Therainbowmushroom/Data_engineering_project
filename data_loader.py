import pandas as pd

file_id = "1zmNAMYzerjdrCZs51MmoWiHfvog-xdi3"  # ID файла на Google Drive
file_url = f"https://drive.google.com/uc?id={file_id}"

df_lung = pd.read_csv(file_url)

df_lung.head(20)
