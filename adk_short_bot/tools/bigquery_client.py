from google.cloud import bigquery
import pandas as pd
import os
from typing import Optional, Dict, Any


def query_bigquery_data(query: str, project_id: Optional[str] = None) -> str:
    """
    Query BigQuery dataset and return results.
    
    Args:
        query (str): SQL query to execute
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        str: Query results formatted as text
    """
    try:
        # Initialize BigQuery client
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
        
        # Execute query
        query_job = client.query(query)
        results = query_job.result()
        
        # Convert to DataFrame for easier handling
        df = results.to_dataframe()
        
        if df.empty:
            return "No results found for your query."
        
        # Format results as readable text
        result_text = f"Query Results ({len(df)} rows):\n\n"
        result_text += df.to_string(index=False, max_rows=50)
        
        if len(df) > 50:
            result_text += f"\n\n... and {len(df) - 50} more rows"
            
        return result_text
        
    except Exception as e:
        return f"Error executing query: {str(e)}"


def get_dataset_info(dataset_id: str, project_id: Optional[str] = None) -> str:
    """
    Get information about tables in a BigQuery dataset.
    
    Args:
        dataset_id (str): BigQuery dataset ID
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        str: Dataset information formatted as text
    """
    try:
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
            
        dataset_ref = client.dataset(dataset_id)
        dataset = client.get_dataset(dataset_ref)
        
        # List tables in dataset
        tables = list(client.list_tables(dataset))
        
        info_text = f"Dataset: {dataset_id}\n"
        info_text += f"Description: {dataset.description or 'No description'}\n"
        info_text += f"Tables ({len(tables)}):\n\n"
        
        for table in tables:
            table_ref = client.get_table(table.reference)
            info_text += f"• {table.table_id}\n"
            info_text += f"  Rows: {table_ref.num_rows:,}\n"
            info_text += f"  Columns: {len(table_ref.schema)}\n"
            info_text += f"  Size: {table_ref.num_bytes / (1024*1024):.2f} MB\n\n"
            
        return info_text
        
    except Exception as e:
        return f"Error getting dataset info: {str(e)}"


def get_table_schema(table_id: str, dataset_id: str, project_id: Optional[str] = None) -> str:
    """
    Get schema information for a specific table.
    
    Args:
        table_id (str): BigQuery table ID
        dataset_id (str): BigQuery dataset ID  
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        str: Table schema formatted as text
    """
    try:
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
            
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        
        schema_text = f"Table: {dataset_id}.{table_id}\n"
        schema_text += f"Rows: {table.num_rows:,}\n"
        schema_text += f"Columns: {len(table.schema)}\n\n"
        schema_text += "Schema:\n"
        
        for field in table.schema:
            schema_text += f"• {field.name} ({field.field_type})"
            if field.mode != "NULLABLE":
                schema_text += f" [{field.mode}]"
            if field.description:
                schema_text += f" - {field.description}"
            schema_text += "\n"
            
        return schema_text
        
    except Exception as e:
        return f"Error getting table schema: {str(e)}" 