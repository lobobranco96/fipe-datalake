HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
        "Referer": "https://www.webmotors.com.br",
        "Connection": "keep-alive"
    }
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import os

def extractt_app():
    def brands():
        url = "https://pt.wikipedia.org/wiki/Categoria:Ve%C3%ADculos_por_marca"
        response = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(response.text, "html.parser")
        brand = soup.find('div', class_="mw-category mw-category-columns")

        # Necessario para ser usado na consulta em wikipedia
        vehicule_brand = [
            model.get_text().strip()
            .replace("Veículos da ", "").split(' (')[0]
            for group in brand.find_all("div", class_="mw-category-group")  # Encontra todos os grupos de categoria
            for model in group.find_all("li")  # Itera por cada item da lista (<li>)
            if " veículos da " in model.get_text().lower()  # Filtra itens com " veículos da "
        ]

        brands_wikipedia = [brand.split(' (')[0]
                        .replace(" ", "_")
                        .replace("š", "s")
                        for brand in vehicule_brand]

        with open('/opt/airflow/dags/python/data/marca_modelo/brands_wikipedia.json', 'w', encoding='utf-8') as file:
            json.dump(brands_wikipedia, file, ensure_ascii=False, indent=4)

    def model(brand):
        url = f"https://pt.wikipedia.org/wiki/Categoria:Ve%C3%ADculos_da_{brand}"
        response = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(response.text, "html.parser")
        # Tentar extrair modelos da primeira estrutura
        if brand == "Ford" or brand == "Volkswagen":
            models = soup.find_all('div', class_="mw-category mw-category-columns")
            vehicule_brand_model = []

            # Verificando se encontrou algo
            if models:
                for model_section in models:
                    # Para cada seção, encontramos as listas de modelos
                    items = model_section.find_all("li")
                    for item in items:
                        model_name = item.get_text(strip=True)
                        # Filtra para garantir que apenas os modelos sejam capturados
                        if len(model_name.split()) > 1:  # Filtra para garantir que são modelos e não categorias
                            vehicule_brand_model.append(
                                model_name
                                .replace(f"{brand} ", "")
                                .replace(f"{brand}-", "")
                                .split(' (')[0]
                                .replace("á", "a").replace("é", "e")
                                .replace("í", "i").replace("ó", "o")
                                .replace("ú", "u").replace('ò', 'o')
                                .replace(' ', '-')
                                .lower()
                            )
            return vehicule_brand_model[8:]
        else:
            models = soup.find('div', class_="mw-category mw-category-columns")
            vehicule_brand_model = []
            if models:
                vehicule_brand_model = [
                    model.get_text(strip=True)
                    .replace(f"{brand} ", "")
                    .replace(f"{brand}-", "").replace("Oroch", "Duster-Oroch")
                    .replace("á", "a").replace("é", "e")
                    .replace("í", "i").replace("ó", "o")
                    .replace("ú", "u").replace('ò', 'o')
                    .replace(' ', '-')
                    .lower().replace("alfa-romeo-", "").replace("aston-martin-", "").replace("jac-", "").replace("land-rover-", "").replace().split('-(')[0]  # .lower() pode ser usado se necessário
                    for model in models.find_all("li")
                ]

        # Tentar extrair modelos da segunda estrutura, se a primeira falhar
        if not vehicule_brand_model:  # Verifica se a lista está vazia
            models2 = soup.find('div', class_="mw-category")
            if models2:
                vehicule_brand_model = [
                    model.get_text(strip=True)
                    .replace(f"{brand} ", "")
                    .replace(f"{brand}-", "").replace("Oroch", "Duster-Oroch")
                    .replace("á", "a").replace("é", "e")
                    .replace("í", "i").replace("ó", "o")
                    .replace("ú", "u").replace('ò', 'o')
                    .replace(' ', '-')
                    .lower()
                    for model in models2.find_all("li")
                ]

        return vehicule_brand_model

    def get_year(brand, model):
        """Busca os anos disponíveis para um modelo"""
        url = f"https://www.webmotors.com.br/tabela-fipe/carros/{brand}/{model}"
        response = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(response.text, "html.parser")

        year_list = soup.find(class_="ItemList__list")
        if year_list:
            car_year_list = [year.get_text(strip=True) for year in year_list.find_all("a")]
            return car_year_list
        return []

    brands()
    caminho = "/opt/airflow/dags/python/data/marca_modelo/brands_wikipedia.json"
    # Abrir e carregar o arquivo JSON
    with open(caminho, 'r', encoding='utf-8') as file:
        data = json.load(file)

    brands_models = []
    for name in data:
        try:
            #vehicle_model = model(name)
            brands_models.append({"Brand": name.lower().replace("_", "-"), "Model": model(name)})
        except Exception as e:
            print(f"An error occurred for brand '{name}': {e}")

    with open('/opt/airflow/dags/python/data/marca_modelo/brands_models.json', 'w', encoding='utf-8') as file:
        json.dump(brands_models, file, ensure_ascii=False, indent=4)

    caminho_diretorio = "/opt/airflow/dags/python/data/marca_modelo/marca_modelo_ano"

    caminho_entrada = "/opt/airflow/dags/python/data/marca_modelo/brands_models.json"
    # Abrir e carregar o arquivo JSON
    with open(caminho_entrada, 'r', encoding='utf-8') as file:
        data1 = json.load(file)

# Lista de arquivos já existentes no diretório
    arquivos_existentes = set(os.listdir(caminho_diretorio))

# Itera sobre as marcas no arquivo JSON
    for item in data1:
        brand = item['Brand']
        models = item['Model']

        # Verifica se o arquivo JSON da marca já existe
        if f"{brand}.json" in arquivos_existentes:
            print(f"Arquivo {brand}.json já existe. Pulando para a próxima marca...")
            continue

        # Lista para armazenar os dados antes de criar o DataFrame
        brand_data = []

        for model in models:
            # Substitua esta função pela implementação real que obtém os anos
            year = get_year(brand, model)

            if year:
                # Adiciona os dados do modelo à lista
                brand_data.append({"Brand": brand, "Model": model, "Year": year})
            else:
                # Caso não tenha ano, apenas ignore o modelo
                continue

        # Se houver dados para essa marca
        if brand_data:
            # Cria um DataFrame com os dados da marca
            df = pd.DataFrame(brand_data)

            # Salva o DataFrame em formato JSON
            caminho_arquivo = os.path.join(caminho_diretorio, f"{brand}.json")
            df.to_json(caminho_arquivo, orient='index', force_ascii=False)
            print(f"Arquivo {brand}.json salvo com sucesso!")


if __name__ == "__main__":

  extractt_app()