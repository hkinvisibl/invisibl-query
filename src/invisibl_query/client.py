import requests
import os
import logging
from .utils import get_aws_role, extract_metadata, MetadataExtractionError

logger = logging.getLogger(__name__)

class QueryClient:
    def __init__(self):
        # Fetch API URL and AUTH_TOKEN from environment variables
        self.cohort_api_base_url = os.getenv("COHORT_API_BASE_URL")
        self.auth_token = os.getenv("AUTH_TOKEN")
        self.project = os.getenv("PROJECT")
        
        if not self.cohort_api_base_url or not self.auth_token or not self.project:
            logger.critical("Configuration missing: Check environment variables.")
            raise RuntimeError("Application configuration failed.")
        
    def execute(self, query: str):
        try:
            # Metadata extraction
            logger.info("Processing query request...") 
            payload = extract_metadata(query)

            headers={
                "Accept": "application/json",
                "Content-Type":"application/json",
                "Cookie": self.auth_token}

            logger.info("Executing query...")
            response = requests.post(
                f"{self.cohort_api_base_url}/projects/{self.project}/cohorts/query",
                headers=headers,
                json={"data":payload},
                timeout=(4, 900)
            )

            try:
                body = response.json()
            except ValueError:
                logger.debug("Invalid JSON response: %s", response.text)
                return {"error": "Invalid response from query execution"}

            if not response.ok:
                if response.status_code == 401:
                    logger.debug("Authentication failed for internal API call.")
                    return {"error": "User authentication failed."}

                if isinstance(body, dict):
                    status_obj = body.get("status", {})
                if isinstance(status_obj, dict) and not status_obj.get("ok", True):
                    error_msg = status_obj.get("error", {}).get("details", {}).get("err")
                    if error_msg:
                        logger.debug(f"Error: {error_msg}")
                        return {"error": error_msg}
                    
            return body

        except MetadataExtractionError as e:
            logger.warning(f"Validation failure: {str(e)}")
            return {"error": "The provided query is invalid or lacks permissions."}

        except requests.exceptions.Timeout:
            logger.debug("Downstream service timeout.")
            return {"error": "The request took too long to process."}

        except requests.exceptions.RequestException:
            # Generic log for all network issues (Connection, HTTP Errors, etc)
            logger.debug("Network communication failure.")
            return {"error": "Service temporarily unavailable."}

        except Exception:
            # Catch-all for unexpected logic errors
            logger.exception("Internal system error.") 
            return {"error": "An internal error occurred."}
        
    
    def list_cohorts(self):
        try:           
            role = get_aws_role()
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Cookie": self.auth_token
            }

            logger.info("Fetching cohorts... ")
            response = requests.get(
                f"{self.cohort_api_base_url}/projects/{self.project}/cohorts",
                params={"role":role},
                headers=headers,
                timeout=(4, 90)
            )

            try:
                body = response.json()
            except ValueError:
                logger.debug("Invalid JSON response: %s", response.text)
                return {"error": "Invalid response from query execution"}

            if not response.ok:
                if response.status_code == 401:
                    logger.error("Authentication failed for internal API call.")
                    return {"error": "User authentication failed."}

                if isinstance(body, dict):
                    status_obj = body.get("status", {})
                if isinstance(status_obj, dict) and not status_obj.get("ok", True):
                    error_msg = status_obj.get("error", {}).get("details", {}).get("err")
                    if error_msg:
                        logger.debug(f"Error: {error_msg}")
                        return {"error": error_msg}
                    
            return body
        
        except requests.exceptions.Timeout:
            logger.debug("ListCohorts API request timed out.")
            return {"error": "The request took too long to process."}

        except requests.exceptions.RequestException:
            logger.debug("Network communication failure calling ListCohorts API.")
            return {"error": "Service temporarily unavailable."}

        except Exception:
            logger.exception("Internal system error.")
            return {"error": "An internal error occurred."}