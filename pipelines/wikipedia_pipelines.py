def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page....", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # check if the request is successful

        return response.text
    except requests.RequestException as e:
        print(f"An error occured: {e}")

def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, features="html.parser")
    table = soup.find_all("table", attrs={"class":"wikitable"})[1]

    table_rows = table.find_all('tr')

    return table_rows

def extract_wikipedia_data(**kwargs):
    import pandas as pd
    url = kwargs["url"]
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank':i,
            'stadium':tds[0].text,
            'capacity':tds[1].text,
            'region':tds[2].text,
            'country':tds[3].text,
            'city':tds[4].text,
            'images':tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE" ,
            'home_team':tds[6].text
        }
        data.append(values)
    
    data_df = pd.DataFrame(data)
    data_df.to_csv("data/output.csv", index=False)
    return data_df