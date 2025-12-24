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
import random
from cache import CachedPageLoader  
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
    page_loader : CachedPageLoader
    async def get_soup(self, url: str ) -> tuple[bool, BeautifulSoup]:
        
        cached, page = await self.page_loader.get(url)
        return cached, BeautifulSoup(page, 'html.parser')
    async def extrair_tudo(self, url : str) -> list[Instituto]:
        cached, soup = await self.get_soup(url)
        institutos = []
        lista = soup.find(class_="lista-oferecimento").find_all(class_="item")
        for intituto in lista:
            url = intituto.find("a")["href"]
            if url.rsplit("/",1)[-1] in ["IC", "FEEC", "IFGW", "IMECC", "IE", "IB", "FEM"]:
                institutos.append(await self.extrair_instituto(url))
        return institutos
    
    async def extrair_instituto(self, url: str) -> Instituto:
        print("Extraindo:", url.rsplit("/",1)[-1])
        cached, soup = await self.get_soup(url)
        nome = soup.find("h1").text.split("-")[0].strip()
        diciplinas = []
        pool = []
        qt_added = 0
        qt_requested = 0
        for dis in soup.find_all(class_="disciplina"):
            dis_url = dis.find("a")["href"]
            pool.append(self.extrair_disciplina(dis_url))
            qt_added+=1
            if(qt_added >= 5):
                reults = await asyncio.gather(*pool)
                for cached, disciplina in reults:
                    if not cached:
                        qt_requested +=1
                    diciplinas.append(disciplina)
                pool = []
                qt_added = 0
            if qt_requested >= 5:
                qt_requested = 0
                await asyncio.sleep(1)
        for cached, disciplina in await asyncio.gather(*pool):
            diciplinas.append(disciplina)
        return Instituto(nome, diciplinas)

    async def extrair_disciplina(self, url: str) -> tuple[bool, Disciplina]:
        sleep_time = 2
        while True:
            try: 
                print("Iniciado:", url.rsplit("/",2)[-2] +"/"+ url.rsplit("/",2)[-1])
                cached, soup = await self.get_soup(url)
                turmas = list(map(self.extrair_turma, soup.find_all(class_="panel")))
                codigo = soup.find("h1").text.split("-")[0].strip()
                nome = soup.find("h1").text.split("-")[1].strip()
                print("Completo:",url.rsplit("/",2)[-2] +"/"+ url.rsplit("/",2)[-1])
                break
            except Exception as e:
                print("Erro ao extrair disciplina, tentando novamente:", url)
                print(e)
                self.page_loader.client = RetryClient(retry_options=retry_options)
                sleep_time = sleep_time*random.uniform(1, 2)  
                await asyncio.sleep(sleep_time*random.uniform(1, 2))
        return cached, Disciplina(codigo, nome, turmas)
    
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

def save_data_to_csv(data : list[Instituto], filename : str) -> None:
    file_exists = os.path.isfile(filename)
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        if not file_exists:
            writer.writerow(['Instituto','Disciplina', 'Nome', 'Turma', 'Dia da Semana', 'Horário Inicio', 'Horário Fim', 'Sala', 'Docentes'])
        
        for instituto in data:
            for disciplina in instituto.diciplinas:
                for turma in disciplina.turmas:
                    for aula in turma.aulas:
                        writer.writerow([instituto.nome, disciplina.codigo, disciplina.nome, turma.nome, aula.dia_semana, aula.horario.inicio, aula.horario.fim, aula.sala, ', '.join(turma.docentes), ', '.join(map(str, turma.reservas))])
    
    print(f"Dados salvos em {filename} com sucesso.")


async def main() -> None:
    base_url = 'https://www.dac.unicamp.br/portal/caderno-de-horarios/2025/2/S/G/'
    # print(load_data_from_csv("./aulas.csv"))
    trace_config = TraceConfig()
    trace_config.on_request_start.append(on_request_start)
    async with RetryClient(retry_options=retry_options, trace_configs=[trace_config])as session:
        loader = CachedPageLoader(session)
        crowler = Crowler(loader)
        tudo = await crowler.extrair_tudo(base_url)
        save_data_to_json(tudo, "./save2025s2.csv");
    


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(main())