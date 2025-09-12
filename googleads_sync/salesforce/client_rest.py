import os
from simple_salesforce import Salesforce, SalesforceLogin

def get_sf():
    session_id = os.getenv("SF_SESSION_ID")
    instance_url = os.getenv("SF_INSTANCE_URL")
    if session_id and instance_url:
        return Salesforce(instance_url=instance_url, session_id=session_id)
    username = os.getenv("SF_USERNAME")
    password = os.getenv("SF_PASSWORD")
    token = os.getenv("SF_SECURITY_TOKEN")
    domain = os.getenv("SF_DOMAIN", "login")
    if not all([username, password, token]):
        raise RuntimeError("Salesforce credentials missing. Provide SF_SESSION_ID+SF_INSTANCE_URL or SF_USERNAME+SF_PASSWORD+SF_SECURITY_TOKEN")
    session_id, instance = SalesforceLogin(
        username=username,
        password=password,
        security_token=token,
        domain=domain,
    )
    return Salesforce(session_id=session_id, instance=instance)

def soql_query(soql: str):
    sf = get_sf()
    return sf.query_all(soql)
