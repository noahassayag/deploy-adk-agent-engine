from google.adk.agents import Agent

from adk_short_bot.prompt import ROOT_AGENT_INSTRUCTION
from adk_short_bot.tools import (
    query_bigquery_data, get_dataset_info, get_table_schema, list_datasets, search_tables,
    authenticate_user, secure_company_count, check_user_permissions
)

root_agent = Agent(
    name="go401_expert",
    model="gemini-2.0-flash",
    description="A 401go expert with enterprise-grade user authentication and permissions",
    instruction=ROOT_AGENT_INSTRUCTION,
    tools=[
        authenticate_user,
        check_user_permissions,
        secure_company_count,
        list_datasets, 
        search_tables, 
        query_bigquery_data, 
        get_dataset_info, 
        get_table_schema
    ],
)
