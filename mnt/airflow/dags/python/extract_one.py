import requests
from bs4 import BeautifulSoup
import json

HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
        "Referer": "https://
        "Connection": "keep-alive"
    }


def extract_one():
  """
  Função principal que executa todo o fluxo de extração de dados.

  1. Chama a função `brands()` para extrair as marcas de veículos da Wikipédia.
  2. Carrega o arquivo JSON com as marcas extraídas e, para cada marca,
      chama a função `model(brand)` para extrair os modelos disponíveis.
  3. Organiza os dados extraídos em um dicionário e os armazena em um arquivo JSON.
  """

  def brands():
      """
      Extrai as marcas de veículos da categoria "Veículos por marca" na Wikipédia.

      - Faz uma requisição HTTP para a URL com a lista de marcas.
      - Usa o BeautifulSoup para extrair os nomes das marcas.
      - Limpa e formata os nomes das marcas para o uso posterior.
      - Salva as marcas em um arquivo JSON.

      Retorna:
          None
      """

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

      with open('/opt/airflow/dags/python/data/primeira_parte/brands_wikipedia.json', 'w', encoding='utf-8') as file:
          json.dump(brands_wikipedia, file, ensure_ascii=False, indent=4)

  def model(brand):
      """
      Extrai os modelos de veículos para uma marca específica a partir da Wikipédia.

      - Faz uma requisição HTTP para a URL de modelos da marca na Wikipédia.
      - Usa BeautifulSoup para extrair a lista de modelos.
      - Filtra e formata os nomes dos modelos para um formato padronizado.

      Parâmetros:
          brand (str): Nome da marca de veículos para a qual os modelos serão extraídos.

      Retorna:
          list: Lista de modelos de veículos extraídos para a marca fornecida.
      """

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
                  .lower()
                  .replace("alfa-romeo-", "")
                  .replace("aston-martin-", "")
                  .replace("jac-", "")
                  .replace("land-rover-", "")
                  .split('-(')[0]  # .lower() pode ser usado se necessário
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

  brands()
  caminho = "/opt/airflow/dags/python/data/primeira_parte/brands_wikipedia.json"
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

  with open('/opt/airflow/dags/python/data/primeira_parte/brands_models.json', 'w', encoding='utf-8') as file:
      json.dump(brands_models, file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    extract_one()
