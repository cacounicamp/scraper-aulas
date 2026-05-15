import asyncio
from bs4 import BeautifulSoup 
from dataclasses import dataclass
import csv
import os
from dataclasses_json import dataclass_json
import random
from cache import CachedPageLoader  
from datetime import datetime


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
    horas_distancia: int
    turmas: list[Turma]


@dataclass_json
@dataclass
class Instituto: 
    nome: str
    diciplinas: list[Disciplina]
    
@dataclass_json
@dataclass
class CadernoDeHorario:
    ano: int
    semestre: int
    institutos: list[Instituto]

def log(*args, **kwargs ) -> None:
    print( datetime.now(),"-" , *args, **kwargs)
## Coletador de informação da Dac

@dataclass
class Crowler:
    page_loader : CachedPageLoader 
    def __init__(self, page_loader : CachedPageLoader | None = None) -> None:
        if page_loader is None:
            self.page_loader = CachedPageLoader()
        else:
            self.page_loader = page_loader
    async def get_soup(self, url: str ) -> tuple[bool, BeautifulSoup]:
        print(url)
        cached, page = await self.page_loader.get(url)
        return cached, BeautifulSoup(page, 'html.parser')

    async def extrair_tudo(self, url: str) -> list[Disciplina]:
        cached, soup = await self.get_soup(url)
        lista = soup.find(class_="prefixos-disciplinas").find_all("a")
        urls_relativas = [a.get("href", "").strip() for a in lista]

        todas_disciplinas = []
        for rel in urls_relativas:
            abs_url = url.rsplit('/', 1)[0] + '/' + rel
            disciplinas_pagina = await self.extrair_disciplinas(abs_url)
            todas_disciplinas.extend(disciplinas_pagina)   # junta as listas
        return todas_disciplinas

    async def extrair_disciplinas(self, url: str) -> list[Disciplina]:
        cached, soup = await self.get_soup(url)

        # Procura todos os elementos <h2> que tenham um id começando com "disc-"
        # (padrão observado no exemplo: id="disc-bc182")
        h2_list = soup.find_all("h2", id=lambda x: x and x.startswith("disc-"))

        disciplinas = []
        for h2 in h2_list:
            # --- Código da disciplina ---
            texto_h2 = h2.get_text(strip=True)               # "BC182 - Biologia Celular I"
            partes = texto_h2.split(" - ", 1)                # separa apenas na primeira ocorrência
            codigo = partes[0].strip() if partes else ""

            # --- Horas de Atividades à Distância ---
            horas_distancia = 0
            # A partir do <h2>, procuramos o próximo <h3> com texto "Carga Horária"
            # Usamos find_next para não depender de ser irmão direto
            elemento_atual = h2
            while elemento_atual:
                carga_h3 = elemento_atual.find_next("h3", string="Carga Horária")
                if carga_h3 is None:
                    break
                p_carga = carga_h3.find_next_sibling("p")
                if p_carga:
                    strong_elem = p_carga.find(
                        "strong",
                        string="Total de Horas de Atividades à Distância:"
                    )
                    if strong_elem and strong_elem.next_sibling:
                        valor_str = strong_elem.next_sibling.strip()
                        try:
                            horas_distancia = int(valor_str)
                        except ValueError:
                            horas_distancia = 0
                break  # encontrou o bloco de carga horária para este h2

            disciplinas.append(Disciplina(codigo=codigo, nome=texto_h2, horas_distancia=horas_distancia, turmas=[]))

        return disciplinas
    
    
def save_caderno_to_json(caderno : CadernoDeHorario, filename : str) -> None:
    with open(filename, 'w', encoding='utf-8') as f:
        value = CadernoDeHorario.schema().dumps(caderno, many=True, ensure_ascii=False)
        f.write(value)
    
def save_data_to_json(data : list[Instituto], filename : str) -> None:
    with open(filename, 'a', encoding='utf-8') as f:
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
    
    log(f"Dados salvos em {filename} com sucesso.")


async def main() -> None:
    try:
        cadernos = []
        crowler = Crowler()
        base_url = f'https://www.dac.unicamp.br/sistemas/catalogos/grad/catalogo2026/disciplinas/index.html'
        tudo = await crowler.extrair_tudo(base_url)
        print(tudo)
        #save_caderno_to_json(cadernos, f"./cadernoshorario.json")
    finally:
        await crowler.page_loader.client.close()    


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(main())