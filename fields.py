CONTACT_FIELDS_DRC = {
    "date_of_birth": "STRING",
    "opt_in_date": "TIMESTAMP",
    "opt_out_date": "TIMESTAMP",
    "optout_reason": "STRING",
    "first_name": "STRING",
    "surname": "STRING",
    "about": "STRING",
    "age_range": "STRING",
    "alerts": "STRING",
    "gender": "STRING",
    "healthcare_worker_identification": "STRING",
    "job": "STRING",
    "suburb": "STRING",
    "manufacturer": "STRING",
    "manufacturer_other": "STRING",
    "town_or_city": "STRING",
}
CONTACT_FIELDS_IC = {
}
GROUP_CONTACT_FIELDS = {"group_uuid": "STRING", "contact_uuid": "STRING"}
GROUP_FIELDS = {"name": "STRING", "uuid": "STRING"}
FLOWS_FIELDS = {"labels": "STRING", "name": "STRING", "uuid": "STRING"}
FLOW_RUNS_FIELDS = {
    "modified_on": "TIMESTAMP",
    "responded": "BOOLEAN",
    "contact_uuid": "STRING",
    "flow_uuid": "STRING",
    "exit_type": "STRING",
    "created_at": "TIMESTAMP",
    "exited_on": "TIMESTAMP",
    "id": "INTEGER",
}
FLOW_RUN_VALUES_FIELDS = {
    "input": "STRING",
    "time": "TIMESTAMP",
    "category": "STRING",
    "name": "STRING",
    "value": "STRING",
    "run_id": "INTEGER",
}
