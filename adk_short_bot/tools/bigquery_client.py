from google.cloud import bigquery
import pandas as pd
import os
from typing import Optional, Dict, Any


def list_datasets(project_id: Optional[str] = None) -> str:
    """
    List all datasets available in the BigQuery project.
    
    Args:
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        str: List of datasets formatted as text
    """
    try:
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
            
        # List all datasets in the project
        datasets = list(client.list_datasets())
        
        if not datasets:
            return "No datasets found in your project."
        
        info_text = f"Available Datasets ({len(datasets)}):\n\n"
        
        for dataset in datasets:
            dataset_ref = client.get_dataset(dataset.reference)
            # Don't count tables for performance - just show basic info
            
            info_text += f"• {dataset.dataset_id}\n"
            info_text += f"  Description: {dataset_ref.description or 'No description'}\n"
            info_text += f"  Location: {dataset_ref.location}\n"
            info_text += f"  Created: {dataset_ref.created.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
        return info_text
        
    except Exception as e:
        return f"Error listing datasets: {str(e)}"


def search_tables(dataset_id: str, search_term: str, project_id: Optional[str] = None, limit: int = 50) -> str:
    """
    Search for tables in a dataset that contain a specific term in their name.
    
    Args:
        dataset_id (str): BigQuery dataset ID
        search_term (str): Term to search for in table names
        project_id (str, optional): Google Cloud project ID
        limit (int): Maximum number of results to return
        
    Returns:
        str: Search results formatted as text
    """
    try:
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
            
        dataset_ref = client.dataset(dataset_id)
        tables = list(client.list_tables(dataset_ref))
        
        # Filter tables that contain the search term
        matching_tables = []
        for table in tables:
            if search_term.lower() in table.table_id.lower():
                matching_tables.append(table)
                if len(matching_tables) >= limit:
                    break
        
        if not matching_tables:
            return f"No tables found containing '{search_term}' in dataset {dataset_id}."
        
        result_text = f"Found {len(matching_tables)} tables containing '{search_term}' in {dataset_id}:\n\n"
        
        for table in matching_tables:
            table_ref = client.get_table(table.reference)
            result_text += f"• {table.table_id}\n"
            result_text += f"  Rows: {table_ref.num_rows:,}\n"
            result_text += f"  Columns: {len(table_ref.schema)}\n"
            result_text += f"  Size: {table_ref.num_bytes / (1024*1024):.2f} MB\n\n"
        
        if len(tables) > limit:
            result_text += f"... and potentially more tables (showing first {limit} matches)"
            
        return result_text
        
    except Exception as e:
        return f"Error searching tables: {str(e)}"


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
        
        try:
            # Try to convert to DataFrame for easier handling
            df = results.to_dataframe()
            
            if df.empty:
                return "No results found for your query."
            
            # Format results as readable text
            result_text = f"Query Results ({len(df)} rows):\n\n"
            result_text += df.to_string(index=False, max_rows=50)
            
            if len(df) > 50:
                result_text += f"\n\n... and {len(df) - 50} more rows"
                
            return result_text
            
        except Exception as df_error:
            # Fallback: if DataFrame conversion fails, process raw results
            rows = list(results)
            if not rows:
                return "No results found for your query."
            
            # Get column names from schema
            column_names = [field.name for field in results.schema]
            
            result_text = f"Query Results ({len(rows)} rows):\n\n"
            
            # Add header
            result_text += " | ".join(column_names) + "\n"
            result_text += "-" * (len(" | ".join(column_names))) + "\n"
            
            # Add data rows (limit to 50)
            for i, row in enumerate(rows[:50]):
                row_data = []
                for value in row:
                    if value is None:
                        row_data.append("NULL")
                    else:
                        row_data.append(str(value))
                result_text += " | ".join(row_data) + "\n"
            
            if len(rows) > 50:
                result_text += f"\n... and {len(rows) - 50} more rows"
                
            return result_text
        
    except Exception as e:
        return f"Error executing query: {str(e)}"


def get_dataset_info(dataset_id: str, project_id: Optional[str] = None, limit: int = 50) -> str:
    """
    Get information about tables in a BigQuery dataset (limited for performance).
    
    Args:
        dataset_id (str): BigQuery dataset ID
        project_id (str, optional): Google Cloud project ID
        limit (int): Maximum number of tables to show details for
        
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
        total_tables = len(tables)
        
        info_text = f"Dataset: {dataset_id}\n"
        info_text += f"Description: {dataset.description or 'No description'}\n"
        info_text += f"Total Tables: {total_tables:,}\n\n"
        
        if total_tables > limit:
            info_text += f"Showing first {limit} tables (use search_tables to find specific tables):\n\n"
            tables_to_show = tables[:limit]
        else:
            info_text += f"Tables:\n\n"
            tables_to_show = tables
        
        for table in tables_to_show:
            table_ref = client.get_table(table.reference)
            info_text += f"• {table.table_id}\n"
            info_text += f"  Rows: {table_ref.num_rows:,}\n"
            info_text += f"  Columns: {len(table_ref.schema)}\n"
            info_text += f"  Size: {table_ref.num_bytes / (1024*1024):.2f} MB\n\n"
        
        if total_tables > limit:
            info_text += f"... and {total_tables - limit:,} more tables\n"
            info_text += f"Use search_tables function to find specific tables by name."
            
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