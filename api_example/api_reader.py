import pandas as pd
import requests


api_url = 'https://search.idigbio.org/v2/search/records'
headers = {
    "Content-Type": "application/json"
}

families = ['asteraceae', 'rosaceae', 'fabaceae']


for family_name in families:
    params = {'rq': {
        'scientificname': {'type': 'exists'}, 'family': family_name}, 
        'limit': 2000}

    response = requests.get(api_url, json=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        items = data.get('items', [])
        df = pd.DataFrame(items)
        filename = f'{family_name}_api.csv'
        df.to_csv(filename, index=False)
        print(f"Для {family_name} получено {len(df)} записей")
        print(df.head())

    else:
        print(f"Ошибка для {family_name}: {response.status_code}")


