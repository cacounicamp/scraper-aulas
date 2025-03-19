import aiohttp
import asyncio
from bs4 import BeautifulSoup 
from dataclasses import dataclass
from aiohttp import ClientSession
import logging
import sys
from types import SimpleNamespace

from aiohttp import ClientSession, TraceConfig, TraceRequestStartParams
from aiohttp_retry import RetryClient, JitterRetry
import csv
import os
import time
from dataclasses_json import dataclass_json

handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(handlers=[handler])
logger = logging.getLogger(__name__)
retry_options = JitterRetry(attempts=100,max_timeout=120)



async def on_request_start(
    session: ClientSession,
    trace_config_ctx: SimpleNamespace,
    params: TraceRequestStartParams,
) -> None:
    current_attempt = trace_config_ctx.trace_request_ctx['current_attempt']
    if(current_attempt > 1):
        logger.warning(params)
    if retry_options.attempts <= current_attempt:
        logger.warning('Wow! We are in last attempt')

# Classes

@dataclass_json
@dataclass 
class HorarioAula:
    inicio: str
    fim: str
    @staticmethod
    def from_str(string:str) -> 'HorarioAula':
        inicio, fim = string.split("-")
        return HorarioAula(inicio.strip(), fim.strip())

@dataclass_json
@dataclass
class Aula:
    dia_semana: str 
    horario: HorarioAula
    sala: str

@dataclass_json
@dataclass
class Turma:
    nome : str
    docentes :  list[str]   
    aulas: list[Aula]
    reservas: list[int]


@dataclass_json
@dataclass
class Disciplina:
    codigo: str
    nome: str
    turmas: list[Turma]


@dataclass_json
@dataclass
class Instituto: 
    nome: str
    diciplinas: list[Disciplina]

## Coletador de informação da Dac

@dataclass
class Crowler:
    session: ClientSession
    async def get_soup(self, url: str ) -> BeautifulSoup:
        
        async with self.session.get(url) as response:
            page = await response.text()
        return BeautifulSoup(page, 'html.parser')
    async def extrair_tudo(self, url : str) -> list[Instituto]:
        soup = await self.get_soup(url)
        institutos = []
        lista = soup.find(class_="lista-oferecimento").find_all(class_="item")
        for intituto in lista:
            institutos.append(await self.extrair_instituto(intituto.find("a")["href"]))
        return institutos
    
    async def extrair_instituto(self, url: str) -> Instituto:
        print("Extraindo:", url.rsplit("/",1)[-1])
        soup = await self.get_soup(url)
        nome = soup.find("h1").text.split("-")[0].strip()
        diciplinas = []
        pool = []
        qt_added = 0
        for dis in soup.find_all(class_="disciplina"):
            dis_url = dis.find("a")["href"]
            pool.append(self.extrair_disciplina(dis_url))
            qt_added+=1
            if(qt_added >= 10):
                diciplinas.extend(await asyncio.gather(*pool))
                pool = []
                qt_added = 0
        diciplinas.extend(await asyncio.gather(*pool))
        return Instituto(nome, diciplinas)
    
    async def extrair_disciplina(self, url: str) -> Disciplina:
        print("Iniciado:", url.rsplit("/",2)[-2] +"/"+ url.rsplit("/",2)[-1])
        soup = await self.get_soup(url)
        turmas = list(map(self.extrair_turma, soup.find_all(class_="panel")))
        codigo = soup.find("h1").text.split("-")[0].strip()
        nome = soup.find("h1").text.split("-")[1].strip()
        print("Completo:",url.rsplit("/",2)[-2] +"/"+ url.rsplit("/",2)[-1])
        return Disciplina(codigo, nome, turmas)
    
    def extrair_turma(self, panel: BeautifulSoup) -> Turma:
        nome =  panel.find(class_="label").text.strip()
        aulas = []       
        reservas = [] 
        for horario in panel.select(".horariosFormatado > li"):
            aulas.append(Aula(horario.find(class_="diaSemana").text, HorarioAula.from_str(horario.find(class_="horarios").text), horario.find(class_="salaAula").text.strip()))
        for reserva in panel.select(".reservas > li"):
            reservas.append(int(reserva.text.split("-")[0]))
        
        docentes = []
        lista_docentes = panel.find(class_="docentes")
        if lista_docentes != None:
            for docente in lista_docentes.find_all("li"):
                docentes.append(docente.text.strip())    
        return Turma(nome, docentes, aulas, reservas)


def save_data_to_json(data : list[Instituto], filename : str) -> None:
    with open(filename, 'a', newline='') as f:
        value = Instituto.schema().dumps(data, many=True)
        f.write(value)


# Salva em arquivo CSV

def save_data_to_csv(data : list[Instituto], filename : str) -> None:
    file_exists = os.path.isfile(filename)
    
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        if not file_exists:
            writer.writerow(['Instituto','Disciplina', 'Nome', 'Turma', 'Dia da Semana', 'Horário Inicio', 'Horário Fim', 'Sala', 'Docentes'])
        
        for instituto in data:
            for disciplina in instituto.diciplinas:
                for turma in disciplina.turmas:
                    for aula in turma.aulas:
                        writer.writerow([instituto.nome, disciplina.codigo, disciplina.nome, turma.nome, aula.dia_semana, aula.horario.inicio, aula.horario.fim, aula.sala, ', '.join(turma.docentes), ', '.join(map(str, turma.reservas))])
    
    print(f"Dados salvos em {filename} com sucesso.")

# Precisa atualizar esse codigo 
def load_data_from_csv(filename):
    data : list[Instituto] = []
    
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        _ = next(reader) 
        
        for row in reader:
            instituto = None
            disciplina = None
            turma = None
            aula = None
            
            instituto_nome = row[0]
            disciplina_nome = row[1]
            turma_nome = row[2].split()[-1]  
            dia_semana = row[3]
            horario_inicio = row[4]
            horario_fim = row[5]
            sala = row[6]
            docentes = row[7].split(', ')
            
            for i in data:
                if i.nome == instituto_nome:
                    instituto = i
                    break
            
            if not instituto:
                instituto = Instituto(instituto_nome, [])
                data.append(instituto)
            
            for d in instituto.diciplinas:
                if d.nome == disciplina_nome:
                    disciplina = d
                    break
            
            if not disciplina:
                disciplina = Disciplina(disciplina_nome,[])
                instituto.diciplinas.append(disciplina)
            
            for t in disciplina.turmas:
                if t.nome == turma_nome:
                    turma = t
                    break
            
            if not turma:
                turma = Turma(turma_nome,[], [])
                disciplina.turmas.append(turma)
            
            aula = Aula(dia_semana,HorarioAula(horario_inicio, horario_fim), sala)
            turma.aulas.append(aula)
            turma.docentes.extend(docentes)
    return data

async def main() -> None:
    base_url = 'https://www.dac.unicamp.br/portal/caderno-de-horarios/2024/2/S/G/IC'
    # print(load_data_from_csv("./aulas.csv"))
    trace_config = TraceConfig()
    trace_config.on_request_start.append(on_request_start)
    async with RetryClient(retry_options=retry_options, trace_configs=[trace_config])as session:
        crowler = Crowler(session)
        save_data_to_json( [await crowler.extrair_instituto(base_url)], "./save2024s2-2.json");
    


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(main())