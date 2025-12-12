import pandas as pd


RENAME_MAPS = {
    "enrollments": {
        # Enrollment record
        "Id": "enr_id",
        "Name": "enr_name",
        "Original_Created_Date__c": "enr_created_date",
        "Submission_Date__c": "enr_submission_date",
        "Effective_Date__c": "enr_effective_date",
        "Enrollment_Status__c": "enr_status",
        "Terminated_Date__c": "enr_terminated_date",
        "Account__c": "account_id_ref",
        # Account (via CVO provider / enrollment)
        "Account__r.NPI__pc": "account_npi",
        "Account__r.Name": "account_name",
        "Account__r.Account_Status__c": "account_status",
        "Account__r.Credentialing_Stage__pc": "account_cred_stage",
        "Account__r.Primary_Practice_State__pc": "account_state",
        # Owner
        "Owner.Id": "enr_owner_id",
        "Owner.Name": "enr_owner_name",
        # Payer Network
        "Payer_Network__c": "payer_network_id",
        "Payer_Network__r.Id": "payer_network_id_ref",
        "Payer_Network__r.Name": "payer_network_name",
        "Payer_Network__r.Owner.Id": "payer_network_owner_id",
        "Payer_Network__r.Owner.Name": "payer_network_owner_name",
        "Payer_Network__r.Default_Enrollment_Status__c": "payer_network_default_enr_status",
    },
    "licenses": {
        "Id": "license_id",
        "Name": "license_name",
        "License_Number__c": "license_number",
        "Effective_Expiration_Date__c": "sf_license_expiration_date",
        "Issue_Date__c": "sf_license_issue_date",
        "CVO_Provider_Data__r.Account__r.NPI__pc": "npi",
        "CVO_Provider_Data__r.Account__r.Name": "account_name",
        "CVO_Provider_Data__r.Account__r.Account_Status__pc": "account_status",
        "CVO_Provider_Data__r.Account__r.Credentialing_Stage__pc": "account_cred_stage",
    },
    "accounts": {
        # --- Core Account Fields ---
        "Id": "account_id",
        "NPI__pc": "account_npi",
        "FirstName": "account_first_name",
        "LastName": "account_last_name",
        "Primary_Practice_State__pc": "account_state",
        "Account_Status__pc": "account_status",
        "Credentialing_Stage__pc": "account_cred_stage",
        "Primary_License__pc": "account_primary_license",
        # --- CVO Fields ---
        "CVO_Credentialing_Listener_Status__pc": "account_cvo_listener_status",
        "Active_CVO_Provider_Data__pr.CVO_First_Name__c": "cvo_first_name",
        "Active_CVO_Provider_Data__pr.CVO_Last_Name__c": "cvo_last_name",
        "Active_CVO_Provider_Data__pr.Credentialing_Status_Update_Date__c": "cvo_cred_status_update_date",
        "Active_CVO_Provider_Data__pr.Last_Outreach_Reason__c": "cvo_last_outreach_reason",
        "Active_CVO_Provider_Data__pr.CVO_CAQH_ID__c": "cvo_caqh_id",
        "Active_CVO_Provider_Data__pr.Attestation_Date__c": "cvo_attestation_date",
        # --- EPD Provider Facts ---
        "EPD_Provider_Facts_Record__pr.Payer_Exclusions__c": "account_payer_exclusions",
        "EPD_Provider_Facts_Record__pr.Contract_Signed__c": "account_contract_signed",
    },
}


def format_fields(dataset_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Format fields in the DataFrame for the specified dataset."""
    rename_map = RENAME_MAPS.get(dataset_name)
    if rename_map is None:
        raise ValueError(f"Unknown dataset_name '{dataset_name}'.")

    return df.rename(columns=rename_map)
