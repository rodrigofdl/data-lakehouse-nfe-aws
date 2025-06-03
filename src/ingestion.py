import requests

API_URL = "https://api.portaldatransparencia.gov.br/api-de-dados/notas-fiscais"
API_KEY = ""
HEADERS = {"accept": "*/*", "chave-api-dados": API_KEY}


def requisition_nfe(organ_code: str, page_number: int) -> list[dict]:
    parameters = {"codigoOrgao": organ_code, "pagina": page_number}

    response = requests.get(API_URL, params=parameters, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def get_nfe_data(organ_code: str, year_emission: str) -> list[dict]:
    all_nfe: list[dict] = []
    page_number = 1

    while True:
        try:
            api_response = requisition_nfe(organ_code, page_number)

            if not api_response:
                break

            filtered_nfe = [
                item
                for item in api_response
                if item.get("dataEmissao", "").endswith("/" + year_emission)
            ]

            all_nfe.extend(filtered_nfe)
            print(
                f"Página {page_number} - {len(filtered_nfe)} registros de 2025 encontrados"
            )

            page_number += 1

        except requests.exceptions.HTTPError as http_err:
            print(f"Erro HTTP: {http_err}")
            break
        except requests.exceptions.RequestException as req_err:
            print(f"Erro de conexão: {req_err}")
            break
        except Exception as e:
            print(f"Erro inesperado: {e}")
            break

    return all_nfe


if __name__ == "__main__":
    get_nfe_data(organ_code="3600", year_emission="2025")
