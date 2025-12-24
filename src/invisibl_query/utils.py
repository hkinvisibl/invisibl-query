import boto3
from sql_metadata import Parser
from typing import Dict, Any
from botocore.exceptions import BotoCoreError, ClientError

sts = boto3.client('sts')

class MetadataExtractionError(Exception):
    """Custom exception for metadata extraction failures."""
    pass

def get_aws_role()->str:
    try:
        identity = sts.get_caller_identity()   
        arn = str(identity.get('Arn', ''))
        if not arn:
            raise MetadataExtractionError("Identity ARN missing from provider.")

        parts = arn.split(":")
        if len(parts) < 6:
            raise MetadataExtractionError(f"Invalid ARN format: {arn}")

        resource = parts[5]

        if resource.startswith("assumed-role/"):
            segments = resource.split("/")
            if len(segments) >= 3:
                return segments[1]
            raise MetadataExtractionError(f"Malformed assumed-role ARN: {arn}")
        
        if resource.startswith("role/"):
            segments = resource.split("/")
            if len(segments) >= 2:
                return segments[1]
            raise MetadataExtractionError(f"Malformed role ARN: {arn}")

        if resource.startswith("user/"):
            segments = resource.split("/")
            if len(segments) >= 2:
                return segments[1]
            raise MetadataExtractionError(f"Malformed user ARN: {arn}")
        
        raise MetadataExtractionError("ARN does not contain a valid assumed-role.")
    except (BotoCoreError, ClientError) as e:
        raise MetadataExtractionError("Authentication provider error.") from e    

def extract_metadata(query: str) -> Dict[str, Any]:
    # Get AWS Identity
    role = get_aws_role()
   
    # SQL Table name(s) extraction
    try:
        parser = Parser(query)
        tables = parser.tables
    except Exception as e:
        raise MetadataExtractionError("Failed to parse SQL syntax.") from e

    if not tables:
        raise MetadataExtractionError("No valid tables identified in request query.")

    return {
        "query": query,
        "role": role,
        "tables": tables
    }