import requests


def get_nfe_data():
    url = "https://api.portaldatransparencia.gov.br/api-de-dados/notas-fiscais"
    key_api = ""

    params = {"codigoOrgao": "36000", "pagina": 1}
    headers = {"accept": "*/*", "chave-api-dados": key_api}

    response = requests.get(url, params=params, headers=headers)

    dados = response.json()

    dados


if __name__ == "__main__":
    get_nfe_data()
