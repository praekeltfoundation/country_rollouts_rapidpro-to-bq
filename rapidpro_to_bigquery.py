from temba_client.v2 import TembaClient
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest
import os

from datetime import datetime

from fields import (
    CONTACT_FIELDS_DRC, CONTACT_FIELDS_IC, GROUP_CONTACT_FIELDS, FLOWS_FIELDS,
    FLOW_RUNS_FIELDS, FLOW_RUN_VALUES_FIELDS, GROUP_FIELDS)

RAPIDPRO_URL = "https://country-rollouts-rapidpro-prd.govcloud-k8s.prd-p6t.org/"
RAPIDPRO_TOKEN_DRC = os.environ["RAPIDPRO_TOKEN_DRC"]
RAPIDPRO_TOKEN_IC = os.environ["RAPIDPRO_TOKEN_IC"]
BQ_KEY_PATH = "/bigquery/bq_credentials.json"
BQ_DATASETS = {
    "drc": "cluster-infra-govcloud-prd.drc_rapidpro",
    "ic": "cluster-infra-govcloud-prd.ivory_coast_rapidpro"
}
clients = {
    "drc": TembaClient(RAPIDPRO_URL, RAPIDPRO_TOKEN_DRC),
    "ic": TembaClient(RAPIDPRO_URL, RAPIDPRO_TOKEN_IC)
}
fields = {
    "drc": CONTACT_FIELDS_DRC,
    "ic": CONTACT_FIELDS_IC
}

credentials = service_account.Credentials.from_service_account_file(
    BQ_KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

bigquery_client = bigquery.Client(
    credentials=credentials, project=credentials.project_id,
)

def log(text):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {text}")


def get_contact_wa_urn(contact):
    wa_urn = " "
    for rapidpro_urn in contact.urns:
        if "whatsapp" in rapidpro_urn:
            urn = rapidpro_urn.split(":")[1]
            wa_urn = f"+{urn}"
        else:
            wa_urn = "1"
    return wa_urn


def get_groups(rapidpro_client):
    rapidpro_groups = rapidpro_client.get_groups().all(retry_on_rate_exceed=True)

    groups = []
    for group in rapidpro_groups:
        groups.append({"uuid": group.uuid, "name": group.name})

    return groups


def get_contacts_and_contact_groups(rapidpro_client, CONTACT_FIELDS, last_contact_date=None):
    rapidpro_contacts = rapidpro_client.get_contacts().all(
        retry_on_rate_exceed=True
    )

    contacts = []
    group_contacts = []
    for contact in rapidpro_contacts:
        record = {
            "uuid": contact.uuid,
            "modified_on": contact.modified_on.isoformat(),
            "name": contact.name,
            "urn": get_contact_wa_urn(contact),
        }

        for group in contact.groups:
            group_contacts.append(
                {"contact_uuid": contact.uuid, "group_uuid": group.uuid}
            )

        for field, value in contact.fields.items():
            if field in CONTACT_FIELDS.keys():
                record[field] = value

        contacts.append(record)

    return contacts, group_contacts


def get_last_record_date(table, field):
    query = f"select EXTRACT(DATETIME from max({field})) from {BQ_DATASET}.{table};"
    for row in bigquery_client.query(query).result():
        if row[0]:
            return str(row[0].strftime("%Y-%m-%dT%H:%M:%S.%fZ"))


def get_flows(rapidpro_client):
    rapidpro_flows = rapidpro_client.get_flows().all(retry_on_rate_exceed=True)

    records = []
    for flow in rapidpro_flows:
        records.append(
            {
                "uuid": flow.uuid,
                "name": flow.name,
                "labels": [label.name for label in flow.labels],
            }
        )
    return records


def get_flow_runs(flows, rapidpro_client, last_contact_date=None):
    records = []
    value_records = []

    for flow in flows:
        for run_batch in rapidpro_client.get_runs(flow=flow["uuid"]).iterfetches(retry_on_rate_exceed=True):
            for run in run_batch:

                exited_on = None
                if run.exited_on:
                    exited_on = run.exited_on.isoformat()
                records.append(
                    {
                        "id": run.id,
                        "flow_uuid": run.flow.uuid,
                        "contact_uuid": run.contact.uuid,
                        "responded": run.responded,
                        "created_at": run.created_on.isoformat(),
                        "modified_on": run.modified_on.isoformat(),
                        "exited_on": exited_on,
                        "exit_type": run.exit_type,
                    }
                )

                for value in run.values.values():
                    value_records.append(
                        {
                            "run_id": run.id,
                            "value": str(value.value),
                            "category": value.category,
                            "time": value.time.isoformat(),
                            "name": value.name,
                            "input": value.input,
                        }
                    )

    return records, value_records


def upload_to_bigquery(BQ_DATASET, table, data, fields):
    # if table in ["flows", "groups"]:
    schema = []
    for field, data_type in fields.items():
        schema.append(bigquery.SchemaField(field, data_type))
    job_config = bigquery.LoadJobConfig(
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        max_bad_records=1,
        autodetect=False
    )
    # else:
    #
    #
    #     job_config = bigquery.LoadJobConfig(
    #         source_format="NEWLINE_DELIMITED_JSON",
    #         write_disposition="WRITE_APPEND",
    #         max_bad_records=1,
    #         autodetect=True
    #     )

    job = bigquery_client.load_table_from_json(
        data, f"{BQ_DATASET}.{table}", job_config=job_config
    )
    try:
        job.result()
    except BadRequest as e:
        for e in job.errors:
            print('ERROR: {}'.format(e['message']))


if __name__ == "__main__":
    # last_contact_date_contacts = get_last_record_date("contacts_raw", "modified_on")
    # last_contact_date_flows = get_last_record_date("flow_runs", "created_at")
    for country in ["drc", "ic"]:
        log("Start")
        print("Fetching flows")
        flows = get_flows(rapidpro_client=clients[country])
        print("Fetching flow runs and values")
        flow_runs, flow_run_values = get_flow_runs(flows, rapidpro_client=clients[country])
        log("Fetching groups...")
        groups = get_groups(rapidpro_client=clients[country])
        log(f"Groups: {len(groups)}")
        log("Fetching contacts and contact groups...")
        contacts, group_contacts = get_contacts_and_contact_groups(rapidpro_client=clients[country], CONTACT_FIELDS=fields[country])
        log(f"Contacts: {len(contacts)}")
        log(f"Group Contacts: {len(group_contacts)}")

        tables = {
            "groups": {
                "data": groups,
                "fields": GROUP_FIELDS},
            "contacts_raw": {
                "data": contacts,
                "fields": fields[country],
            },
            "group_contacts": {
                "data": group_contacts,
                "fields": GROUP_CONTACT_FIELDS,
            },
            "flows": {
                "data": flows,
                "fields": FLOWS_FIELDS,
            },
            "flow_runs": {
                "data": flow_runs,
                "fields": FLOW_RUNS_FIELDS,
            },
            "flow_run_values": {
                "data": flow_run_values,
                "fields": FLOW_RUN_VALUES_FIELDS,
            }
        }

        for table, data in tables.items():
            rows = data["data"]
            log(f"Uploading {len(rows)} {table}")

            upload_to_bigquery(BQ_DATASETS[country], table, rows, data.get("fields"))

    log("Done")
