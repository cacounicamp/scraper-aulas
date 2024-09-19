from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup 
import csv


@dataclass 
class HorarioAula:
    inicio: str
    fim: str

    @staticmethod
    def from_str(string:str) -> 'HorarioAula':
        inicio, fim = string.split("-")
        return HorarioAula(inicio.strip(), fim.strip())

@dataclass
class Aula:
    dia_semana: str 
    horario: HorarioAula
    sala: str

@dataclass
class Turma:
    nome : str
    docentes :  list[str]
    aulas: list[Aula]
    def __init__(self, panel : BeautifulSoup):
        self.aulas = []
        for horario in panel.find_all(class_="horariosFormatado"):
            self.aulas.append(Aula(horario.find(class_="diaSemana").text, HorarioAula.from_str(horario.find(class_="horarios").text), horario.find(class_="salaAula").text.strip()))
        self.nome =  panel.find(class_="label").text.strip()
        self.docentes = []
        for docente in panel.find(class_="docentes").find_all("li"):
            self.docentes.append(docente.text.strip())    

@dataclass
class Disciplina:
    nome: str
    turmas: list[Turma]
    
    @staticmethod
    def from_url(url: str) -> 'Disciplina':
        turmas = []
        response = requests.get(url)
        page = response.text
        soup = BeautifulSoup(page, 'html.parser')
        nome = soup.find("h1").text.split("-")[0].strip()

        for panel in soup.find_all(class_="panel"):
            turmas.append(Turma(panel))
        return Disciplina(nome, turmas)

@dataclass
class Instituto: 
    nome: str
    diciplinas: list[Disciplina]
    @staticmethod
    def from_url(url: str) -> 'Instituto':
        response = requests.get(url)
        page = response.text
        soup = BeautifulSoup(page, 'html.parser')
        print("start", url)
        nome = soup.find("h1").text.split("-")[0].strip()
        diciplinas = []
        for dis in soup.find_all(class_="disciplina"):
            diciplinas.append(Disciplina.from_url(dis.find("a")["href"]))
            print(dis.find("a")["href"])
        return Instituto(nome, diciplinas)


def save_data_to_csv(data : list[Instituto], filename : str) -> None:
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        writer.writerow(['Instituto', 'Disciplina', 'Turma', 'Dia da Semana', 'Horário Inicio', 'Horário Fim', 'Sala', 'Docentes'])
        
        for instituto in data:
            for disciplina in instituto.diciplinas:
                for turma in disciplina.turmas:
                    for aula in turma.aulas:
                        writer.writerow([instituto.nome, disciplina.nome, disciplina.nome + " " + turma.nome, aula.dia_semana, aula.horario.inicio, aula.horario.fim, aula.sala, ', '.join(turma.docentes)])
    
    print(f"Data saved to {filename} successfully.")



def main() -> None:
    institutos = []
    base_url = 'https://www.dac.unicamp.br/portal/caderno-de-horarios/2024/2/S/G'
    response = requests.get(base_url)
    page = response.text
    soup = BeautifulSoup(page, 'html.parser')

    s_institutos = soup.find(class_="lista-oferecimento").find_all(class_="item")
    for intituto in s_institutos:
        institutos.append(Instituto.from_url(intituto.find("a")["href"]))
    save_data_to_csv(institutos, "./save.csv");



main()

