import requests
from .utils import extract_metadata

class QueryClient:
    def __init__(self):
        self.api_url = "http://localhost:3000/datascience/api/v1/projects/tre/cohorts/query"

    def execute(self, query: str):
        # Get AWS identity and table metadata
        payload = extract_metadata(query)

        print("payload - ", payload)
        # payload["arn"] = 'arn:aws:sts::554248189203:assumed-role/quark-dev-wt-omop-analysis-sttgh-role/i-083b0b912b506ee78'
       
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "gravity-userid":"hardik.kesari@invisibl.io",
            "gravity-userinfo-token":"{\"tokenSecretName\" :\"user-hardik-kesari-invisibl-io-dffccc766\"}"
        }

        try:
            response = requests.post(
                f"{self.api_url}", 
                headers=headers,
                json=payload,
                timeout=(2,900)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}