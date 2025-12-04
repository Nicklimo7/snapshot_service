import pandas as pd


def format_fields(dataset_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Format fields in the DataFrame for the specified dataset.

    Args:
        dataset_name (str): The name of the dataset. Options include "enrollments".
        df (pd.DataFrame): The DataFrame to format.

    Returns:
        pd.DataFrame: The formatted DataFrame.
    """
    enr_rename_map = {
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
        "Account__r.NPI__pc": "account_npi",  # same target as before
        "Account__r.Name": "account_name",  # same
        "Account__r.Account_Status__c": "account_status",  # new, name reflects field
        "Account__r.Credentialing_Stage__pc": "account_cred_stage",
        "Account__r.Primary_Practice_State__pc": "account_state",  # keep account_state
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
    }

    licenses_rename_map = {
        "Id": "license_id",
        "Name": "license_name",
        "License_Number__c": "license_number",
        "Effective_Expiration_Date__c": "sf_license_expiration_date",
        "Issue_Date__c": "sf_license_issue_date",
        "CVO_Provider_Data__r.Account__r.NPI__pc": "npi",
        "CVO_Provider_Data__r.Account__r.Name": "account_name",
        "CVO_Provider_Data__r.Account__r.Account_Status__pc": "account_status",
        "CVO_Provider_Data__r.Account__r.Credentialing_Stage__pc": "account_cred_stage",
    }

    if dataset_name == "enrollments":
        df = df.rename(columns=enr_rename_map)
    elif dataset_name == "licenses":
        df = df.rename(columns=licenses_rename_map)

    return df
