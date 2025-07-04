from google.cloud import bigquery
import os
from typing import Optional, Dict, Any, List
from dataclasses import dataclass


@dataclass
class UserContext:
    """Represents the current user's context and permissions"""
    user_id: str
    email: str
    role: str
    company_ids: List[str]
    plan_ids: List[str]
    permissions: List[str]
    is_super_admin: bool = False


def get_user_context(user_email: str, project_id: Optional[str] = None) -> Optional[UserContext]:
    """
    Get user context and permissions from BigQuery based on email.
    
    Args:
        user_email (str): User's email address
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        UserContext or None if user not found
    """
    try:
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
        
        # Query user information from your database
        query = f"""
        SELECT 
            u.id as user_id,
            u.email,
            u.role,
            u.is_super_admin,
            ARRAY_AGG(DISTINCT uc.company_id) as company_ids,
            ARRAY_AGG(DISTINCT up.plan_id) as plan_ids,
            ARRAY_AGG(DISTINCT p.permission_name) as permissions
        FROM dev_dataset.go401_dev_accounts_user u
        LEFT JOIN dev_dataset.go401_dev_user_companies uc ON u.id = uc.user_id
        LEFT JOIN dev_dataset.go401_dev_user_plans up ON u.id = up.user_id  
        LEFT JOIN dev_dataset.go401_dev_user_permissions up_perm ON u.id = up_perm.user_id
        LEFT JOIN dev_dataset.go401_dev_permissions p ON up_perm.permission_id = p.id
        WHERE u.email = @user_email
        GROUP BY u.id, u.email, u.role, u.is_super_admin
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ]
        )
        
        results = client.query(query, job_config=job_config).to_dataframe()
        
        if results.empty:
            return None
            
        row = results.iloc[0]
        
        return UserContext(
            user_id=str(row['user_id']),
            email=row['email'],
            role=row['role'],
            company_ids=row['company_ids'] or [],
            plan_ids=row['plan_ids'] or [],
            permissions=row['permissions'] or [],
            is_super_admin=bool(row['is_super_admin'])
        )
        
    except Exception as e:
        print(f"Error getting user context: {str(e)}")
        return None


def check_permission(user_context: UserContext, required_permission: str) -> bool:
    """
    Check if user has a specific permission.
    
    Args:
        user_context (UserContext): User's context
        required_permission (str): Permission to check
        
    Returns:
        bool: True if user has permission
    """
    if user_context.is_super_admin:
        return True
        
    return required_permission in user_context.permissions


def get_user_data_scope(user_context: UserContext) -> str:
    """
    Get the SQL WHERE clause for data scoping based on user permissions.
    
    Args:
        user_context (UserContext): User's context
        
    Returns:
        str: SQL WHERE clause to limit data access
    """
    if user_context.is_super_admin:
        return ""  # No restrictions for super admin
    
    # Role-based data scoping
    if user_context.role == "participant":
        # Participants can only see their own data
        return f"WHERE participant_email = '{user_context.email}'"
    
    elif user_context.role == "company_admin":
        # Company admins can see data for their companies
        if user_context.company_ids:
            company_list = "', '".join(user_context.company_ids)
            return f"WHERE company_id IN ('{company_list}')"
    
    elif user_context.role == "plan_admin":
        # Plan admins can see data for their plans
        if user_context.plan_ids:
            plan_list = "', '".join(user_context.plan_ids)
            return f"WHERE plan_id IN ('{plan_list}')"
    
    elif user_context.role == "advisor":
        # Advisors can see data for companies they manage
        if user_context.company_ids:
            company_list = "', '".join(user_context.company_ids)
            return f"WHERE company_id IN ('{company_list}')"
    
    # Default: no access
    return "WHERE 1=0"


# Global variable to store current user context (will be set per session)
current_user_context: Optional[UserContext] = None


def set_user_context(user_email: str, project_id: Optional[str] = None) -> bool:
    """
    Set the global user context for the current session.
    
    Args:
        user_email (str): User's email address
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        bool: True if user context was set successfully
    """
    global current_user_context
    current_user_context = get_user_context(user_email, project_id)
    return current_user_context is not None


def get_current_user_context() -> Optional[UserContext]:
    """Get the current user context."""
    return current_user_context 