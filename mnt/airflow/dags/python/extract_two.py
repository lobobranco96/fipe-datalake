import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import time

HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
        "Connection": "keep-alive"
    }

def extract_two():
    """
    Função principal que executa o processo de extração dos anos disponíveis para cada modelo de veículo.

    1. Chama a função `get_model_year()` para cada marca e seus respectivos modelos.
    2. Para cada modelo, chama a função `get_year()` para extrair os anos disponíveis.
    3. Organiza os dados de marcas, modelos e anos em um dicionário.
    4. Salva os dados extraídos em um arquivo JSON para cada marca.

    Retorna:
        None
    """

    def get_year(session, brand, model):
        """
        Extrai os anos disponíveis para um modelo específico na Webmotors.

        - Faz uma requisição HTTP para a página do modelo na Webmotors.
        - Usa BeautifulSoup para extrair a lista de anos disponíveis para o modelo.

        Parâmetros:
            brand (str): Nome da marca do veículo.
            model (str): Nome do modelo do veículo.

        Retorna:
            list: Lista de anos de fabricação disponíveis para o modelo.
        """
        """ URL REMOVIDO"""
        try:
            response = session.get(url)
            if response.status_code == 401:
                print(f"Erro 401: Autenticação necessária. Esperando antes de tentar novamente...")
                time.sleep(15)  # Espera 10 segundos antes de tentar novamente
                response = session.get(url, timeout=15)  # Repetir a requisição após o tempo de espera
                if response.status_code == 401:
                    erros_401[brand].append(model)
                    print(f"Erro 401 persistente para {brand} {model}. Pulando o modelo.")
                    #return None

            if response.status_code != 200:
                raise Exception(f"Erro ao acessar a URL {url}. Status Code: {response.status_code}")


            soup = BeautifulSoup(response.text, "html.parser")
            year_list = soup.find(class_="ItemList__list")
            if year_list:
                car_year_list = [year.get_text(strip=True) for year in year_list.find_all("li")]
                return car_year_list
            return None  # Se não encontrar anos, retorna None

        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição: {e}")
            return None  # Retorna None em caso de erro de requisição


    def get_model_year(brand, models):
        """
        Obtém os anos de cada modelo de uma marca e salva os dados em um arquivo JSON.

        - Para cada modelo, chama a função `get_year()` para extrair os anos.
        - Organiza os dados em um dicionário com as chaves "Brand", "Model" e "Year".
        - Salva os dados em um arquivo JSON no caminho especificado.

        Parâmetros:
            brand (str): Nome da marca de veículos.
            models (list): Lista de modelos de veículos para a marca.

        Retorna:
            None
        """
        brand_model_year_path = "/opt/airflow/dags/python/data/segunda_parte"
        brand_data = {"Brand": [], "Model": [], "Year": []}

        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        })
        for model in models:
            try:
                year = get_year(session, brand, model)
                if year:
                    brand_data['Brand'].append(brand)
                    brand_data['Model'].append(model)
                    brand_data['Year'].append(year)
                    print(f"Ano para {brand} {model}: {year}")
            except Exception as e:
                print(f"Erro ao obter o ano para {brand} {model}: {e}")
                continue  # Pula para o próximo modelo

        if len(brand_data['Year']) == 0:
          print("Dados não encontrados.")
        else:
        # Salvando os dados em um arquivo JSON
          df = pd.DataFrame(brand_data)
          caminho_arquivo = os.path.join(brand_model_year_path, f"{brand}.json")
          print(f"Arquivo {brand}.json salvo com sucesso!")
          df.to_json(caminho_arquivo, orient='index')

    caminho_entrada = "/opt/airflow/dags/python/data/primeira_parte/brands_models.json"
    # Abrir e carregar o arquivo JSON
    with open(caminho_entrada, 'r', encoding='utf-8') as file:
        brands_models_data = json.load(file)

    brand_model_year_path = "/opt/airflow/dags/python/data/segunda_parte"

    # Listar todos os arquivos da pasta
    existing_files = os.listdir(brand_model_year_path)

    # Dicionário para armazenar marcas e modelos com erro 401
    erros_401 = {}

    for item in brands_models_data:
      #if item['Brand'] == 'nissan':
      brand = item['Brand']
      models = item['Model']
      if brand == 'chery':
        brand = 'caoa-chery'
      elif brand == "citroën":
        brand = 'citroen'

      # Verifica se o arquivo da marca já existe
      if f"{brand}.json" not in existing_files:
          print(f"Processando a marca: {brand}")
          erros_401[brand] = []
          get_model_year(brand, models)
      else:
          print(f"O arquivo {brand}.json já está presente. Pulando...")


if __name__ == "__main__":
    extract_two()