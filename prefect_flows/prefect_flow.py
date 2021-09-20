import prefect
import sys
import os

from prefect import task, Flow, Parameter
from prefect.storage import Docker
from prefect.tasks.prefect import StartFlowRun


from gima_common.secrets_tool import get_secrets
from gima_common.oracle_functions import OracleClient
from gima_common.gma_functions import MailClient, MailAttachment
from gima_common.prefect import manual_trigger_adv, post_to_slack

from os.path import exists
from datetime import datetime,timedelta

slack_token = '123'
flow_name = "pbk_loader"
project_name = "gima"
environment = 'ontwikkel'

logger = prefect.context.get("logger")
logger.info('OBK')

config = {
    "omgeving": environment,
    "flow_naam": flow_name,
    "registry": "dev-brm.so.kadaster.nl:5002",
    "project_naam": project_name
}

def get_secret_key_names(db):
    if config['omgeving'] == 'prod' and db == 'STG':
        output = {"user": "oracle-staging-user", "service": "oracle-staging-service",
                  "password": "oracle-staging-password"}
    else:
        output = {"user": "oracle-user", "service": "oracle-service", "password": "oracle-password"}
    return output


def get_sql_script(filename):
    if exists(f'../sql/{filename}'):
        path = f'../sql/{filename}'
    elif exists(f'../prefect_flows/sql/{filename}'):
        path = f'../prefect_flows/sql/{filename}'
    else:
        path = f"/sql/{filename}"
    with open(path) as file:
        data = file.read()
    return data


def run_sql_script(oracle_client, filename, bind_variable=None):
    data = get_sql_script(filename)
    oracle_client.run_query(data, bind_variable, False)


def get_oracle_client(db='prod'):
    secrets = get_secrets()
    secret_keys = get_secret_key_names(db)
    logger.info(f"Maak connectie met database {secrets.get(secret_keys['service'])}")
    oracle_client = OracleClient(service=secrets.get(secret_keys['service']), user=secrets.get(secret_keys['user']),
                                 password=secrets.get(secret_keys['password']))

    return oracle_client


def list_to_string(list):
    if len(list) == 0:
        output = ""
    else:
        output = ";".join(list[0].keys()) + "\n"
        for row in list:
            output = output + ";".join(str(x) for x in row.values()) + "\n"
    return output



@task()
def laden_pbk_cbs():
    oracle_client = get_oracle_client()
    run_sql_script(oracle_client, 'OBK_OV18_MARKER.sql')
    run_sql_script(oracle_client, 'OBK_OV18_OUTPUT.sql')


@task()
def valideren_pbk_cbs():
    oracle_client = get_oracle_client()
    run_sql_script(oracle_client, 'OBK_OV44_MARKER.sql')


@task()
def laden_pbk_basisbestand():
    oracle_client = get_oracle_client()
    run_sql_script(oracle_client, 'OBK_AANTALLEN_PROCS.sql')




relative_path = f"{os.getcwd()}/prefect_flows/sql"
files = {f"{relative_path}/OBK_OV03_MARKER.sql": "/sql/OBK_OV03_MARKER.sql",
         f"{relative_path}/OBK_OV03_OUTPUT.sql": "/sql/OBK_OV03_OUTPUT.sql"
         }

with Flow(config['flow_naam'], storage=Docker(registry_url=config['registry'], files=files,
                                              image_name=f"gima/prefect/{config['omgeving']}/{config['flow_naam']}")
          ) as flow:
    # stand = Parameter('stand (YYYYMMDD) Eerste dag van nieuwe maand', default='20210101', required=True)

    task_laden_pbk_cbs = laden_pbk_cbs
    task_valideren_pbk_cbs = valideren_pbk_cbs(upstream_tasks=[task_laden_pbk_cbs])
    task_laden_pbk_basisbestand = laden_pbk_basisbestand(upstream_tasks=[task_valideren_pbk_cbs])


# LET OP: tijdens ontwikkelen nodig
if __name__ == "__main__":
    # flow.validate()
    # flow.visualize()
    flow.run(parameters={'stand (YYYYMMDD) Eerste dag van nieuwe maand':'20210601'})
