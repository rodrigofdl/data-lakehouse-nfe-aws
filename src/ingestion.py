import requests


def get_nfe_data():
    url = "https://api.portaldatransparencia.gov.br/api-de-dados/notas-fiscais"
    key = ""

    params = {"codigoOrgao": "36000", "pagina": 1}
    headers = {"accept": "*/*", "chave-api-dados": key}

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data_json = response.json()
        return data_json
    except requests.exceptions.HTTPError as http_err:
        print(f"Erro HTTP: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Erro de conexão: {req_err}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


if __name__ == "__main__":
    get_nfe_data()
