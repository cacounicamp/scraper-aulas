from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin, urlparse
import time
from dataclasses import dataclass

@dataclass
class Disciplina:
    id: str
    carga: str
    cred: str
    name: str

@dataclass
class Curso:
    name: str
    data: list[Disciplina]

class Scraper:
    def __init__(self, main_url: str, delay: float = 0.25):
        self.main_url: str = main_url
        self.delay: float = delay

    def get_soup(self, url=None):
        url = url or self.main_url
        response = requests.get(url)
        time.sleep(self.delay)
        return BeautifulSoup(response.content, 'html.parser')
    
    def scrape_disciplina(self, url) -> list[Disciplina]:
        list_disciplinas = []
        soup = self.get_soup(url)

        rows = soup.find_all('tr')

        for row in rows:
            cells = row.find_all('td')

            id = cells[0].find('strong').find('a', class_ = "link").text.strip()
            new = Disciplina(id, cells[1].text, cells[2].text, cells[3].text)
            list_disciplinas.append(new)
            time.sleep(self.delay)
            print(f"scraped {new}: DISCIPLINA")

        return list_disciplinas

    def scrape_curso(self) -> list[Curso]:
        list_cursos = []
        soup = self.get_soup()

        rows = soup.find_all('li', class_ = "accordion-navigation")
        for row in rows:
            curso_name = row.find('a', class_ = "rotulo-curso").text
            link = row.find('a', string = "Currículo Pleno").get('href')
            next_url = urljoin(self.main_url, link)

            curso_disciplinas = self.scrape_disciplina(next_url)
            new = Curso(curso_name, curso_disciplinas)
            list_cursos.append(new)
            time.sleep(self.delay)
            print(f"scraped {new} : CURSO")
        
        return list_cursos
        
#MAIN

url = "https://www.dac.unicamp.br/sistemas/catalogos/grad/catalogo2026/index.html"

scraper = Scraper(url)
cursos = scraper.scrape_curso()

