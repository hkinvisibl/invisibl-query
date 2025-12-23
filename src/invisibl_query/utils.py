import boto3
from sql_metadata import Parser
from typing import Dict, Any, List, cast

sts = boto3.client('sts')

def extract_metadata(query: str) -> Dict[str, Any]:
    # Get AWS Identity
    identity = cast(Dict[str, Any], sts.get_caller_identity())
    
    # SQL Table name(s) extraction
    parser = Parser(query)
    tables = cast(List[str], parser.tables)
    
    return {
        "arn": str(identity.get('Arn', 'unknown')),
        "account": str(identity.get('Account', 'unknown')),
        "tables": tables,
        "query": query
    }