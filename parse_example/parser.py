import pandas as pd
from bs4 import BeautifulSoup
import requests


source = 'https://www.samadm.ru/'
req = requests.get(source)
req.raise_for_status()
print(len(req.text))
req.text

req_soup = BeautifulSoup(req.text, 'html.parser')
req_soup

news = req_soup.select('div.news')

dates = []
text_news_list = []
links = []

for line in news:

    date = line.select_one('time') if line else None
    link_all = line.select_one('a') if line else None
    text_news = link_all.text.strip() if link_all else None
    link = link_all['href'] if link_all else None

    dates.append(date)
    text_news_list.append(text_news)
    links.append(link)

columns = ["date", "link", "text_news"]
df_news = pd.DataFrame(list(zip(dates, links, text_news_list)), 
                       columns=columns)
print(df_news.head())

df_news.to_csv('df_news_samara', index=False)

