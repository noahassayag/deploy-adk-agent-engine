from google.cloud import bigquery
import pandas as pd
from typing import Optional, Dict, Any
from .user_context import get_current_user_context, check_permission, get_user_data_scope, set_user_context


def authenticate_user(user_email: str, project_id: Optional[str] = None) -> str:
    """
    Authenticate and set user context for the session.
    
    Args:
        user_email (str): User's email address
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        str: Authentication status message
    """
    success = set_user_context(user_email, project_id)
    
    if success:
        user_context = get_current_user_context()
        return f"""User authenticated successfully!
        
User: {user_context.email}
Role: {user_context.role}
Companies: {', '.join(user_context.company_ids) if user_context.company_ids else 'None'}
Plans: {', '.join(user_context.plan_ids) if user_context.plan_ids else 'None'}
Permissions: {', '.join(user_context.permissions) if user_context.permissions else 'None'}
Super Admin: {user_context.is_super_admin}

You can now access data according to your permissions."""
    else:
        return f"Authentication failed. User '{user_email}' not found or has no access."


def secure_query_companies(project_id: Optional[str] = None) -> str:
    """
    Query companies data with user permissions applied.
    
    Args:
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        str: Query results formatted as text
    """
    user_context = get_current_user_context()
    if not user_context:
        return "Error: User not authenticated. Please authenticate first using authenticate_user function."
    
    if not check_permission(user_context, "view_companies"):
        return f"Access denied. Your role '{user_context.role}' does not have permission to view companies."
    
    try:
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
        
        # Build query with user data scope
        data_scope = get_user_data_scope(user_context)
        
        if user_context.role == "participant":
            return "Participants cannot view company-level data."
        
        query = f"""
        SELECT 
            id,
            name,
            EIN,
            entity_type,
            contact_name,
            contact_email
        FROM dev_dataset.go401_dev_companies_company
        {data_scope}
        ORDER BY name
        LIMIT 50
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        try:
            df = results.to_dataframe()
            
            if df.empty:
                return "No companies found that you have access to."
            
            result_text = f"Companies you have access to ({len(df)} total):\n\n"
            result_text += df.to_string(index=False, max_rows=50)
            
            return result_text
            
        except Exception:
            # Fallback for raw results
            rows = list(results)
            if not rows:
                return "No companies found that you have access to."
            
            result_text = f"Companies you have access to ({len(rows)} total):\n\n"
            for row in rows:
                result_text += f"• {row['name']} (ID: {row['id']}, EIN: {row['EIN']})\n"
            
            return result_text
        
    except Exception as e:
        return f"Error querying companies: {str(e)}"


def secure_query_participants(company_id: Optional[str] = None, project_id: Optional[str] = None) -> str:
    """
    Query participant data with user permissions applied.
    
    Args:
        company_id (str, optional): Specific company ID to filter
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        str: Query results formatted as text
    """
    user_context = get_current_user_context()
    if not user_context:
        return "Error: User not authenticated. Please authenticate first using authenticate_user function."
    
    if not check_permission(user_context, "view_participants"):
        return f"Access denied. Your role '{user_context.role}' does not have permission to view participants."
    
    try:
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
        
        # Build query with user data scope
        data_scope = get_user_data_scope(user_context)
        
        # Add company filter if specified and user has access
        if company_id:
            if user_context.role != "super_admin" and company_id not in user_context.company_ids:
                return f"Access denied. You don't have access to company ID '{company_id}'."
            
            if data_scope:
                data_scope += f" AND company_id = '{company_id}'"
            else:
                data_scope = f"WHERE company_id = '{company_id}'"
        
        query = f"""
        SELECT 
            id,
            email,
            first_name,
            last_name,
            company_id,
            enrollment_date
        FROM dev_dataset.go401_dev_participants_participant
        {data_scope}
        ORDER BY last_name, first_name
        LIMIT 100
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        try:
            df = results.to_dataframe()
            
            if df.empty:
                return "No participants found that you have access to."
            
            result_text = f"Participants you have access to ({len(df)} total):\n\n"
            result_text += df.to_string(index=False, max_rows=50)
            
            if len(df) > 50:
                result_text += f"\n\n... and {len(df) - 50} more participants"
            
            return result_text
            
        except Exception:
            # Fallback for raw results
            rows = list(results)
            if not rows:
                return "No participants found that you have access to."
            
            result_text = f"Participants you have access to ({len(rows)} total):\n\n"
            for row in rows:
                result_text += f"• {row['first_name']} {row['last_name']} ({row['email']})\n"
            
            return result_text
        
    except Exception as e:
        return f"Error querying participants: {str(e)}"


def secure_company_count(project_id: Optional[str] = None) -> str:
    """
    Count companies the user has access to.
    
    Args:
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        str: Company count message
    """
    user_context = get_current_user_context()
    if not user_context:
        return "Error: User not authenticated. Please authenticate first using authenticate_user function."
    
    if not check_permission(user_context, "view_companies"):
        return f"Access denied. Your role '{user_context.role}' does not have permission to view companies."
    
    try:
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
        
        # Build query with user data scope
        data_scope = get_user_data_scope(user_context)
        
        query = f"""
        SELECT COUNT(*) as company_count
        FROM dev_dataset.go401_dev_companies_company
        {data_scope}
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            count = row['company_count']
            
            if user_context.is_super_admin:
                return f"Total companies in the system: {count}"
            else:
                return f"Companies you have access to: {count}"
        
    except Exception as e:
        return f"Error counting companies: {str(e)}"


def check_user_permissions() -> str:
    """
    Display current user's permissions and context.
    
    Returns:
        str: User permission summary
    """
    user_context = get_current_user_context()
    if not user_context:
        return "Error: User not authenticated. Please authenticate first using authenticate_user function."
    
    result = f"""Current User Context:
    
Email: {user_context.email}
Role: {user_context.role}
User ID: {user_context.user_id}
Super Admin: {user_context.is_super_admin}

Company Access: {', '.join(user_context.company_ids) if user_context.company_ids else 'None'}
Plan Access: {', '.join(user_context.plan_ids) if user_context.plan_ids else 'None'}

Permissions:
{chr(10).join(['• ' + perm for perm in user_context.permissions]) if user_context.permissions else '• No specific permissions'}

Data Scope: {get_user_data_scope(user_context) or 'Full access (Super Admin)'}
"""
    
    return result 