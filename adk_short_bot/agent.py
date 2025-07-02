from google.adk.agents import Agent

from adk_short_bot.prompt import ROOT_AGENT_INSTRUCTION
from adk_short_bot.tools import query_bigquery_data, get_dataset_info, get_table_schema

root_agent = Agent(
    name="401go_expert",
    model="gemini-2.0-flash",
    description="A 401go expert that answers questions based on BigQuery data",
    instruction=ROOT_AGENT_INSTRUCTION,
    tools=[query_bigquery_data, get_dataset_info, get_table_schema],
)
 