import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


API_URL = "https://api.portaldatransparencia.gov.br/api-de-dados/notas-fiscais"
API_KEY = ""


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
)
def request_nfe(organ_code: str, page_number: int) -> list[dict]:
    headers = {"accept": "*/*", "chave-api-dados": API_KEY}
    parameters = {"codigoOrgao": organ_code, "pagina": page_number}

    response = requests.get(API_URL, params=parameters, headers=headers)
    response.raise_for_status()
    return response.json()


def filter_nfe_per_year(api_response: list[dict], year_emission: str) -> list[dict]:
    return [
        nfe
        for nfe in api_response
        if nfe.get("dataEmissao", "").endswith("/" + year_emission)
    ]


def get_nfe_data(organ_code: str, year_emission: str) -> list[dict]:
    all_nfe: list[dict] = []
    page_number = 1

    while True:
        try:
            api_response = request_nfe(organ_code, page_number)

            if not api_response:
                break

            filtered_nfe = filter_nfe_per_year(api_response, year_emission)
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
