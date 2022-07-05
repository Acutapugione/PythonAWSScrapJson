import json
import requests
from bs4 import BeautifulSoup

headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7,uk;q=0.6",
        "cache-control": "max-age=0",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.66 Safari/537.36 Edg/103.0.1264.44"
    }

def get_links_from_wikinews(url='', tmp_name='index'):
    
    req = requests.get(url=url, headers=headers)
    src = req.text
    with open(f'Data/{tmp_name}.html' , 'w', encoding='utf-8') as file:
        file.write(src)
    with open(f'Data/{tmp_name}.html', encoding='utf-8') as file:
        src = file.read()

    soup = BeautifulSoup(src, 'lxml')
    data = []
    latest_news_links = soup.find(class_='latest_news_text').findAll('a')
    for item in latest_news_links:
        data.append({
            "title": item.text,
            "link": f'https://en.wikinews.org/{item.get("href")}'
        })
    return data

def save_data_to_json(data, file_name=''):
    with open(f'Data/{file_name}.json' , 'w') as file:
        json.dump(data, fp=file, indent=4, ensure_ascii=False)
    print(f'Data successful saved to Data/{file_name}.json')

def get_data_from_json(file_name=''):
    with open(f'Data/{file_name}.json') as file:
        return json.load(file)

def load_news(data, folder_path):
    cnt=0
    for item in data:
        cnt+=1
        req = requests.get(url=item.get('link'), headers=headers)
        src = req.text
        with open(f'{folder_path}{cnt}.html', 'w' , encoding='utf-8') as file:
            file.write(src)

def main():
    #data = get_data_from_url(url='https://en.wikinews.org/wiki/Main_Page')
    #save_data_to_json(data, 'data')
    data = get_data_from_json('data')
    load_news(data, 'Data/News/')
   

if __name__ == '__main__':
    main()
