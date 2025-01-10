import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import os

def extract_three(brand):
    """
    Função principal para coletar dados detalhados sobre os veículos de uma marca,
    incluindo modelos, anos e preços da Tabela FIPE.

    Essa função realiza os seguintes passos:
    1. Gera URLs para os detalhes dos veículos de uma marca, utilizando a função `get_url()`.
    2. Para cada modelo e ano, coleta detalhes específicos do veículo, gera URLs e preços da Tabela FIPE.
    3. Salva essas informações em um arquivo CSV.

    Parâmetros:
        brand (str): Nome da marca de veículos a ser processada.

    Retorna:
        None
    """

    def model_year(brand, model, vehicle_year):
        """Busca os modelos de um veículo para um ano específico na Webmotors."""
        key = f"{brand}/{model}/{vehicle_year}"
        url = f"https://www.webmotors.com.br/tabela-fipe/carros/{key}"
        response = requests.get(url, timeout=5)
        soup = BeautifulSoup(response.text, "html.parser")
        model_list = soup.find_all(class_="ItemList__item")
        if model_list:
            models = [
                item.get_text(strip=True)
                .replace(".", "")
                .replace(" ", "-")
                .lower()
                .replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
                for item in model_list
            ]
            return models
        return []

    def fipe(url):
        """Busca o preço FIPE de um veículo a partir da URL fornecida."""
        response = requests.get(url, timeout=5)
        soup = BeautifulSoup(response.text, "html.parser")
        result = soup.find("div", class_="Result")
        if result:
            text = result.get_text(strip=True)
            match = re.search(r"R\$\s?\d{1,3}(?:\.\d{3})*,\d{2}", text)
            return match.group(0) if match else None
        return None

    def create_url(brand, model, year, car_detail):
        """Cria uma URL completa para o veículo, combinando a marca, modelo, ano e detalhes."""
        url = f"https://www.webmotors.com.br/tabela-fipe/carros/{brand}/{model}/{year}/{car_detail}/rj"
        return url

    caminho_entrada = f"/opt/airflow/dags/python/data/segunda_parte/{brand}.json"

    try:
        with open(caminho_entrada, 'r', encoding='utf-8') as file:
            brand_json = json.load(file)
    except FileNotFoundError:
        print(f"Arquivo {brand}.json não encontrado!")
        return
    except json.JSONDecodeError:
        print(f"Erro ao decodificar o arquivo {brand}.json!")
        return

    # Inicializando o dicionário tabela_fipe
    tabela_fipe = {
        "brand": [],       # Lista para as marcas
        "model": [],       # Lista para os modelos
        "year": [],        # Lista para os anos
        "car_detail": [],  # Lista para os detalhes do carro
        "url": [],         # Lista para as URLs
        "fipe_price": []   # Lista para os preços da Fipe
    }

    # Iterar sobre os dados da marca
    for data in brand_json.values():
        brand = data.get('Brand')
        model = data.get('Model')
        years = data.get('Year')

        # Iterar sobre os anos para adicionar os detalhes de cada ano
        for year in years:
          tabela_fipe["brand"].append(brand)
          tabela_fipe["model"].append(model)
          tabela_fipe["year"].append(year)  # Adiciona o ano

            # Obter os detalhes do carro para aquele ano
          car_detail = model_year(brand, model, year)
          for car in car_detail:
            tabela_fipe["car_detail"].append(car)  # Adiciona o detalhamento do carro
            url = create_url(brand, model, year, car)  # Cria a URL
            tabela_fipe["url"].append(url)  # Adiciona a URL
            fipe_price = fipe(url)  # Obtém o preço da Fipe para a URL
            tabela_fipe["fipe_price"].append(fipe_price if fipe_price else 0)

    # Salvar as informações em um arquivo CSV
    caminho_csv = f"/opt/airflow/dags/python/data/terceira_parte/{brand}_fipe.csv"
    df = pd.DataFrame(tabela_fipe)
    df.to_csv(caminho_csv, index=False)
    print(f"CSV para {brand} salvo com sucesso!")

def last_extract():
    """Processa todas as marcas de veículos, gerando CSVs de dados da Tabela FIPE."""
    caminho_diretorio = "/opt/airflow/dags/python/data/segunda_parte"
    arquivos_existentes = set(os.listdir(caminho_diretorio))

    csv_diretorio = "/opt/airflow/dags/python/data/terceira_parte"
    csv_existentes = set(os.listdir(csv_diretorio))

    for brand in arquivos_existentes:
        if brand != '.ipynb_checkpoints' and brand.endswith('.json'):
            brand_name = brand.replace('.json', '')
            # Verifica se o arquivo CSV já existe com o nome correto
            csv_filename = f"{brand_name}_fipe.csv"
            if csv_filename not in csv_existentes:
                print(f"Processando a marca: {brand_name}")
                if brand_name == "audi":
                    pass
                else:
                    extract_three(brand_name)
            else:
                print(f"Arquivo CSV já existe para {brand_name}, ignorando.")
        else:
            print(f"Arquivo ignorado: {brand}")

if __name__ == "__main__":
    last_extract()