import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import os
import time
import random

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
        """
        Busca os modelos de um veículo para um ano específico na Webmotors.

        - Faz uma requisição HTTP para a página do modelo e ano na Webmotors.
        - Usa BeautifulSoup para extrair a lista de modelos disponíveis para aquele ano e modelo.

        Parâmetros:
            brand (str): Nome da marca do veículo.
            model (str): Nome do modelo do veículo.
            vehicle_year (str): Ano do veículo.

        Retorna:
            list: Lista de modelos de veículos disponíveis para o ano e modelo especificado.
        """

        """Busca os modelos de um veículo para um ano específico"""
        key = f"{brand}/{model}/{vehicle_year}"
        "URL REMOVIDO"

        max_retries = 3  # Número máximo de tentativas
        attempt = 0  # Contador de tentativas
        while attempt < max_retries:
            try:
                response = requests.get(url, timeout=5)
                # Verifica se o status da resposta é 200 (OK)
                if response.status_code == 401:
                    print(f"Erro 401: Autenticação necessária para a URL {url}. Tentando novamente em 20 segundos...")
                    attempt += 1
                    time.sleep(20)  # Aguarda 20 segundos antes de tentar novamente
                    continue

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
                else:
                    print(f"Não foram encontrados modelos para {brand} {model} {vehicle_year}.")
                    return []

            except requests.exceptions.RequestException as e:
                # Captura erros relacionados ao request (exemplo: timeout, conexão recusada)
                print(f"Erro de requisição: {e}")
                return []

    def fipe(url):
        """
        Busca o preço FIPE de um veículo a partir da URL fornecida.

        - Faz uma requisição HTTP para a página de detalhes do veículo.
        - Usa BeautifulSoup para extrair o preço FIPE, que é retornado em formato de string.

        Parâmetros:
            url (str): URL do veículo na Webmotors para obter o preço FIPE.

        Retorna:
            str or None: Preço FIPE do veículo (formato R$ XXX,XX) ou `None` caso não encontrado.
        """

        """Busca o preço FIPE de um veículo"""
        max_retries = 3  # Número máximo de tentativas
        attempt = 0  # Contador de tentativas
        while attempt < max_retries:
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 401:
                    print(f"Erro 401: Autenticação necessária para a URL {url}. Tentando novamente em 20 segundos...")
                    attempt += 1
                    time.sleep(20)  # Aguarda 20 segundos antes de tentar novamente
                    continue  # Vai para a próxima tentativa

                soup = BeautifulSoup(response.text, "html.parser")
                result = soup.find("div", class_="Result")

                if result:
                    text = result.get_text(strip=True)
                    match = re.search(r"R\$\s?\d{1,3}(?:\.\d{3})*,\d{2}", text)
                    return match.group(0) if match else None
                else:
                    print(f"Preço não encontrado para a URL {url}.")
                    return None

            except requests.exceptions.RequestException as e:
                # Captura erros relacionados ao request (exemplo: timeout, conexão recusada)
                print(f"Erro de requisição: {e}")
                return None
            except Exception as e:
                # Captura qualquer outro erro (por exemplo, se o código de status não for 200)
                print(f"Erro inesperado: {e}")
                return None

    # Função para criar as URLs
    def create_url(brand, model, year, car_detail):
        """
        Cria uma URL completa para o veículo, combinando a marca, modelo, ano e detalhes.

        - A URL é usada para acessar a página do veículo na Webmotors e obter informações detalhadas.

        Parâmetros:
            brand (str): Nome da marca do veículo.
            model (str): Nome do modelo do veículo.
            year (str): Ano do veículo.
            car_detail (str): Detalhe do veículo (variante específica).

        Retorna:
            str: URL formatada para o veículo específico na Webmotors.
        """

        """Cria a URL do veículo"""
        "URL REMOVIDO"
        return url

    caminho_diretorio = "/opt/airflow/dags/python/data/segunda_parte"

    caminho_entrada = f"/opt/airflow/dags/python/data/segunda_parte/{brand}.json"

    # Carregar os dados do arquivo JSON
    with open(caminho_entrada, 'r', encoding='utf-8') as file:
        brand_json = json.load(file)

    # Inicializando o dicionário tabela_fipe
    tabela_fipe = {
        "brand": [],       # Lista para as marcas
        "model": [],       # Lista para os modelos
        "year": [],        # Lista para os anos
        "car_detail": [],  # Lista para os detalhes do carro
        "url": [],         # Lista para as URLs
        "fipe_price": []   # Lista para os preços da Fipe
    }

    # Iterar sobre os dados da marca BMW
    for data in brand_json.values():
        brand = data.get('Brand')
        model = data.get('Model')
        years = data.get('Year')
        #print(brand)
        for year in years:
            car_details = model_year(brand, model, year)  # Detalhes do carro para o ano
            if car_details and isinstance(car_details, list):
                if len(car_details) > 3:
                    car_details = random.sample(car_details, 3)

                for car_detail in car_details:
                    url = create_url(brand, model, year, car_detail)
                    fipe_price = fipe(url)
                    if fipe_price is not None and fipe_price != "":
                        print(f"{brand} | {model} | {year} | {car_detail} | {fipe_price}")
                        tabela_fipe["fipe_price"].append(fipe_price)
                        tabela_fipe["brand"].append(brand)
                        tabela_fipe["model"].append(model)
                        tabela_fipe["year"].append(year)
                        tabela_fipe["car_detail"].append(car_detail)
                        tabela_fipe["url"].append(url)
            continue

    return pd.DataFrame(tabela_fipe).to_csv(f'/opt/airflow/dags/python/data/terceira_parte/{brand}_fipe.csv')

def last_extract():
    """Processa todas as marcas de veículos, gerando CSVs de dados da Tabela FIPE."""
    caminho_diretorio = "/opt/airflow/dags/python/data/segunda_parte"
    arquivos_existentes = set(os.listdir(caminho_diretorio))

    csv_diretorio = "/opt/airflow/dags/python/data/terceira_parte"
    csv_existentes = set(os.listdir(csv_diretorio))

    for brand in arquivos_existentes:
        if brand != '.ipynb_checkpoints' and brand.endswith('.json'):
            brand_name = brand.replace('.json', '')
            # Verifica se o arquivo CSV já existe
            csv_filename = f"{brand_name}_fipe.csv"
            if csv_filename not in csv_existentes:
                print(f"Processando a marca: {brand_name}")
                extract_three(brand_name)
            else:
                print(f"Arquivo CSV já existe para {brand_name}, ignorando.")
        else:
            print(f"Arquivo ignorado: {brand}")

if __name__ == "__main__":
    last_extract()