import requests


def get_nfe_data(codigo_orgao, ano_emissao):
    url = "https://api.portaldatransparencia.gov.br/api-de-dados/notas-fiscais"
    key = ""

    all_data = []

    while True:
        params = {"codigoOrgao": codigo_orgao, "pagina": 1}
        headers = {"accept": "*/*", "chave-api-dados": key}

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            if not data:
                break

            filtered_data = [
                item
                for item in data
                if item.get("dataEmissao", "").endswith("/" + ano_emissao)
            ]

            all_data.extend(filtered_data)
            print(
                f"Página {params["pagina"]} - {len(filtered_data)} registros de 2025 encontrados"
            )

            params["pagina"] += 1

        except requests.exceptions.HTTPError as http_err:
            print(f"Erro HTTP: {http_err}")
            break
        except requests.exceptions.RequestException as req_err:
            print(f"Erro de conexão: {req_err}")
            break
        except Exception as e:
            print(f"Erro inesperado: {e}")
            break

    return all_data


if __name__ == "__main__":
    get_nfe_data(codigo_orgao="3600", ano_emissao="2025")
