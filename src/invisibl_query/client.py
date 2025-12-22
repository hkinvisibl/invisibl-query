import requests
from .utils import extract_metadata

class QueryClient:
    def __init__(self):
        self.api_url = ""

    def execute(self, sql: str):
        # Get AWS identity and potentially table metadata
        payload = extract_metadata(sql)
        
        try:
            response = requests.post(
                f"{self.api_url}/v1/execute", 
                json=payload,
                timeout=(2,900)
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            return {"error": f"Failed to execute cohort: {str(e)}"}