from bs4 import BeautifulSoup, Tag
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
class Modalidade:
    codigo: str
    title: str
    data: list[Disciplina]

@dataclass
class Curso:
    numero: int
    name: str
    modalidades: list[Modalidade]

class Scraper:
    def __init__(self, main_url: str, delay: float = 0.25):
        self.main_url: str = main_url
        self.delay: float = delay

    def get_soup(self, url=None):
        url = url or self.main_url
        response = requests.get(url)
        time.sleep(self.delay)
        return BeautifulSoup(response.content, 'html.parser')
    
    def parse_modalidades(self, key: str, url: str) -> dict[str, list[Tag]]:
        soup = self.get_soup(url)
        div = soup.find('div', class_ = "small-12 columns pad-content professores")

        rows = div.find_all(key)
        parsed = {}

        for row in rows:
            stuff = []
            title = row.get_text(strip = True)
            curr = row.next_sibling
            while curr and (curr.name != key):
                if isinstance(curr, Tag):
                    stuff.append(curr)
                curr = curr.next_sibling
            
            parsed[title] = stuff

        return parsed
            
    def scrape_disciplina(self, soup) -> list[Disciplina]:
        list_disciplinas = []

        for element in soup:
            rows = element.find_all('tr')

            for row in rows:
                cells = row.find_all('td')

                id = cells[0].find('strong').find('a', class_ = "link").text.strip()
                new = Disciplina(id, cells[1].text, cells[2].text, cells[3].text)
                list_disciplinas.append(new)
                print(f"scraped {new}: DISCIPLINA")

        return list_disciplinas

    def scrape_curso(self) -> list[Curso]:
        cursos_list = []
        soup = self.get_soup()

        rows = soup.find_all('li', class_ = "accordion-navigation")
        for row in rows:
            curso_heading = (row.find('a', class_ = "rotulo-curso").text).split(" - ")
            curso_code = int(curso_heading[0])
            curso_name = curso_heading[1]

            link = row.find('a', string = "Currículo Pleno").get('href')
            next_url = urljoin(self.main_url, link)

            modalidades = self.parse_modalidades('h2', next_url)

            modalidades_list = []
            for title, parsed in modalidades.items():
                disciplinas_list = self.scrape_disciplina(parsed)
                split_title = title.split(" - ")
                modalidade_new = Modalidade(split_title[0], split_title[1], disciplinas_list)
                modalidades_list.append(modalidade_new)

            curso_new = Curso(curso_code, curso_name, modalidades_list)
            cursos_list.append(curso_new)
            time.sleep(self.delay)
            print(f"scraped {curso_new} : CURSO")
        
        return cursos_list
        
#MAIN

url = "https://www.dac.unicamp.br/sistemas/catalogos/grad/catalogo2026/index.html"

scraper = Scraper(url)
cursos = scraper.scrape_curso()

