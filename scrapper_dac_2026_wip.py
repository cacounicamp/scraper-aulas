from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin, urlparse
import time

class tempData:
    def __init__(self, id, carga, cred, name):
        self.id = id
        self.carga = carga
        self.cred = cred
        self.name = name

    def __str__(self):
        return f"{self.id}, {self.carga}, {self.cred}, {self.name}"

url = "https://www.dac.unicamp.br/sistemas/catalogos/grad/catalogo2026/index.html"
res = requests.get(url)

soup = BeautifulSoup(res.content, 'html.parser')

links = list(n.get('href') for n in soup.find_all('a', string = "Currículo Pleno"))

disciplinas = []

for href in links:
    next_url = urljoin(url, href)
    res = requests.get(next_url)
    soup = BeautifulSoup(res.content, 'html.parser')
    rows = soup.find_all('tr')
    
    count = 0
    for n, row in enumerate(rows):
        pot = row.find_all('td')
        id = pot[0].find('strong').find('a', class_ = "link").text.strip()
        new = tempData(id, pot[1].text, pot[2].text, pot[3].text)
        disciplinas.append(new)
        if n > 5:
            break
    
    if len(disciplinas) > 10:
        break

for n in disciplinas:
    print(str(n))
