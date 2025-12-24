import requests
import os
import logging
from .utils import extract_metadata, MetadataExtractionError

logger = logging.getLogger(__name__)

class QueryClient:
    def __init__(self):
        # Fetch API URL and AUTH_TOKEN from environment variables
        self.api_url = os.getenv("EXECUTE_QUERY_API")
        self.auth_token = os.getenv("AUTH_TOKEN")
        
        if not self.api_url or not self.auth_token:
            logger.critical("Configuration missing: Check environment variables.")
            raise RuntimeError("Application configuration failed.")
        
    def execute(self, query: str):
        try:
            # Metadata extraction
            logger.info("Processing query request...") 
            payload = extract_metadata(query)

            print("....", payload)

            headers={
                "Accept": "application/json",
                "Content-Type":"application/json",
                "Cookie": self.auth_token}

            logger.info("Executing query...")
            response = requests.post(
                f"{self.api_url}",
                headers=headers,
                json=payload,
                timeout=(4, 900)
            )

            try:
                body = response.json()
            except ValueError:
                logger.error("Invalid JSON response: %s", response.text)
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
                        logger.error(f"Error: {error_msg}")
                        return {"error": error_msg}
                    
            return body

        except MetadataExtractionError as e:
            logger.warning(f"Validation failure: {str(e)}")
            return {"error": "The provided query is invalid or lacks permissions."}

        except requests.exceptions.Timeout:
            logger.error("Downstream service timeout.")
            return {"error": "The request took too long to process."}

        except requests.exceptions.RequestException:
            # Generic log for all network issues (Connection, HTTP Errors, etc)
            logger.error("Network communication failure.")
            return {"error": "Service temporarily unavailable."}

        except Exception:
            # Catch-all for unexpected logic errors
            logger.exception("Internal system error.") 
            return {"error": "An internal error occurred."}
