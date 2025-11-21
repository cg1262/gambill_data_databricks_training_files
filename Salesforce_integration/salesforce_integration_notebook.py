# Databricks notebook source
# MAGIC %md
# MAGIC # Salesforce Integration Notebook
# MAGIC # Purpose: Push data from Unity Catalog to Salesforce Order object
# MAGIC # Schedule: Via Databricks Job

# COMMAND ----------

# Install required libraries
%pip install requests pandas

# COMMAND ----------

import requests
import json
import pandas as pd
import io
from datetime import datetime
from typing import List, Dict, Tuple
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# ============================================================================
# CONFIGURATION
# ============================================================================

# Azure Key Vault Configuration
KEY_VAULT_URL = dbutils.secrets.get(scope="your-scope", key="key-vault-url")
SALESFORCE_CLIENT_ID = dbutils.secrets.get(scope="your-scope", key="sf-client-id")
SALESFORCE_CLIENT_SECRET = dbutils.secrets.get(scope="your-scope", key="sf-client-secret")
SALESFORCE_USERNAME = dbutils.secrets.get(scope="your-scope", key="sf-username")
SALESFORCE_PASSWORD = dbutils.secrets.get(scope="your-scope", key="sf-password")
SALESFORCE_INSTANCE = "login.salesforce.com"  # Use sandbox: test.salesforce.com for dev

# Unity Catalog Configuration
CATALOG = "bronze_prod"
SCHEMA = "kourier"
TABLE = "nf_saleshdr"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Salesforce Configuration
SALESFORCE_OBJECT = "Order"
EXTERNAL_ID_FIELD = "Parts_Order_Number__c"  # For upsert operations
MAX_WAIT_MINUTES = 30  # Max time to wait for Bulk API job completion

# Field Mapping: Source -> Target
FIELD_MAPPING = {
    "order_number": "Parts_Order_Number__c",
    "bill_to_nbr": "APTS_ERP_Number__c",
    "ship_to_nbr": "Ship_To_Customer__c",
    "transaction_date": "Sales_Order_Date__c",
    "request_date": "Request_Date__c",  # TODO: Add this field to source if needed
    "po_number": "Customer_PO_Number__c"
}

# COMMAND ----------

# ============================================================================
# AUTHENTICATION
# ============================================================================

def get_salesforce_token(client_id: str, client_secret: str, username: str, password: str) -> str:
    """
    Authenticate with Salesforce and retrieve OAuth token
    """
    try:
        auth_url = f"https://{SALESFORCE_INSTANCE}/services/oauth2/token"
        
        payload = {
            "grant_type": "password",
            "client_id": client_id,
            "client_secret": client_secret,
            "username": username,
            "password": password
        }
        
        response = requests.post(auth_url, data=payload, timeout=30)
        response.raise_for_status()
        
        token = response.json().get("access_token")
        logger.info("Successfully authenticated with Salesforce")
        return token
        
    except Exception as e:
        logger.error(f"Failed to authenticate with Salesforce: {str(e)}")
        raise

# COMMAND ----------

# ============================================================================
# DATA EXTRACTION & TRANSFORMATION
# ============================================================================

def read_source_data() -> pd.DataFrame:
    """
    Read data from Unity Catalog source table
    """
    try:
        df = spark.read.table(SOURCE_TABLE).toPandas()
        logger.info(f"Successfully read {len(df)} records from {SOURCE_TABLE}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to read from source table: {str(e)}")
        raise

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform source data to match Salesforce schema
    """
    try:
        # Select and rename columns according to field mapping
        transform_df = df[list(FIELD_MAPPING.keys())].copy()
        transform_df = transform_df.rename(columns=FIELD_MAPPING)
        
        # Data type conversions and cleaning
        # Convert transaction_date to ISO format (YYYY-MM-DD)
        if "Sales_Order_Date__c" in transform_df.columns:
            transform_df["Sales_Order_Date__c"] = pd.to_datetime(
                transform_df["Sales_Order_Date__c"]
            ).dt.strftime("%Y-%m-%d")
        
        # Convert request_date to ISO format if present
        if "Request_Date__c" in transform_df.columns:
            transform_df["Request_Date__c"] = pd.to_datetime(
                transform_df["Request_Date__c"]
            ).dt.strftime("%Y-%m-%d")
        
        # Remove rows with null external ID (required for upsert)
        transform_df = transform_df.dropna(subset=[EXTERNAL_ID_FIELD])
        
        # Handle null values - Salesforce API expects null values to be empty strings
        transform_df = transform_df.fillna("")
        
        logger.info(f"Transformed {len(transform_df)} records")
        return transform_df
        
    except Exception as e:
        logger.error(f"Failed to transform data: {str(e)}")
        raise

# COMMAND ----------

# ============================================================================
# SALESFORCE BULK API 2.0 OPERATIONS
# ============================================================================

def create_bulk_job(token: str, instance_url: str) -> str:
    """
    Create a Salesforce Bulk API 2.0 job for upsert operation
    Returns: job_id
    """
    try:
        url = f"{instance_url}/services/data/v59.0/jobs/upsert"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "object": SALESFORCE_OBJECT,
            "externalIdFieldName": EXTERNAL_ID_FIELD
        }
        
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        job_id = response.json().get("id")
        logger.info(f"Created Bulk API job: {job_id}")
        return job_id
        
    except Exception as e:
        logger.error(f"Failed to create Bulk API job: {str(e)}")
        raise

def upload_bulk_data(token: str, instance_url: str, job_id: str, df: pd.DataFrame) -> None:
    """
    Upload CSV data to Salesforce Bulk API job
    """
    try:
        # Convert DataFrame to CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        url = f"{instance_url}/services/data/v59.0/jobs/upsert/{job_id}/batches"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/csv"
        }
        
        response = requests.put(url, data=csv_data, headers=headers, timeout=60)
        response.raise_for_status()
        
        logger.info(f"Uploaded {len(df)} records to Bulk API job {job_id}")
        
    except Exception as e:
        logger.error(f"Failed to upload data to Bulk API job: {str(e)}")
        raise

def close_bulk_job(token: str, instance_url: str, job_id: str) -> None:
    """
    Close the Bulk API job to start processing
    """
    try:
        url = f"{instance_url}/services/data/v59.0/jobs/upsert/{job_id}"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        payload = {"state": "UploadComplete"}
        
        response = requests.patch(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        logger.info(f"Closed Bulk API job {job_id} for processing")
        
    except Exception as e:
        logger.error(f"Failed to close Bulk API job: {str(e)}")
        raise

def wait_for_job_completion(token: str, instance_url: str, job_id: str, max_wait_minutes: int = 30) -> Tuple[str, dict]:
    """
    Poll Bulk API job status until completion
    Returns: (job_state, job_info)
    """
    try:
        url = f"{instance_url}/services/data/v59.0/jobs/upsert/{job_id}"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        start_time = time.time()
        max_wait_seconds = max_wait_minutes * 60
        
        while True:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            job_info = response.json()
            job_state = job_info.get("state")
            
            logger.info(f"Job {job_id} state: {job_state}")
            logger.info(f"  Successful: {job_info.get('numberSuccessful', 0)}")
            logger.info(f"  Failed: {job_info.get('numberFailed', 0)}")
            logger.info(f"  In Progress: {job_info.get('numberInProgress', 0)}")
            
            if job_state in ["JobComplete", "Aborted", "Failed"]:
                return job_state, job_info
            
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > max_wait_seconds:
                logger.warning(f"Job {job_id} did not complete within {max_wait_minutes} minutes")
                return "Timeout", job_info
            
            # Wait before polling again (exponential backoff, capped at 30 seconds)
            wait_time = min(30, 5 + (elapsed / 60))
            logger.info(f"Waiting {wait_time:.0f} seconds before next poll...")
            time.sleep(wait_time)
        
    except Exception as e:
        logger.error(f"Failed to wait for job completion: {str(e)}")
        raise

def get_failed_records(token: str, instance_url: str, job_id: str) -> List[Dict]:
    """
    Retrieve failed records from completed Bulk API job
    """
    try:
        url = f"{instance_url}/services/data/v59.0/jobs/upsert/{job_id}/failedResults"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/csv"
        }
        
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        
        if not response.text:
            logger.info("No failed records")
            return []
        
        # Parse CSV response
        failed_df = pd.read_csv(io.StringIO(response.text))
        failed_records = failed_df.to_dict(orient="records")
        
        logger.info(f"Retrieved {len(failed_records)} failed records")
        return failed_records
        
    except Exception as e:
        logger.warning(f"Failed to retrieve failed records: {str(e)}")
        return []

def get_instance_url(token: str) -> str:
    """
    Get Salesforce instance URL from token metadata
    """
    try:
        # The instance URL can be extracted from the token response or retrieved from identity endpoint
        identity_url = f"https://{SALESFORCE_INSTANCE}/services/oauth2/tokeninfo?access_token={token}"
        response = requests.get(identity_url, timeout=30)
        response.raise_for_status()
        
        instance_url = response.json().get("instance_url")
        if not instance_url:
            # Fallback: assume standard instance URL format
            instance_url = f"https://{SALESFORCE_INSTANCE.replace('login', '')}"
        
        return instance_url
        
    except Exception as e:
        logger.error(f"Failed to get instance URL: {str(e)}")
        # Fallback to default
        return f"https://yourinstance.salesforce.com"

# COMMAND ----------

# ============================================================================
# ERROR HANDLING & LOGGING
# ============================================================================

def log_results_to_delta(successful: int, failed: int, failed_records: List[Dict], job_id: str):
    """
    Log sync results to a Delta table for auditing and monitoring
    """
    try:
        timestamp = datetime.now()
        
        # Create summary record
        summary_data = [{
            "sync_timestamp": timestamp,
            "job_id": job_id,
            "successful_records": successful,
            "failed_records": failed,
            "total_records": successful + failed,
            "success_rate": round((successful / (successful + failed) * 100), 2) if (successful + failed) > 0 else 0
        }]
        
        summary_df = spark.createDataFrame(summary_data)
        summary_df.write.mode("append").option("mergeSchema", "true") \
            .saveAsTable("default.salesforce_sync_logs", path="/user/hive/warehouse/salesforce_sync_logs")
        
        # Log failed records if any
        if failed_records:
            failed_df = spark.createDataFrame(
                [{
                    "sync_timestamp": timestamp,
                    "job_id": job_id,
                    "record_data": json.dumps(record),
                    "error_message": record.get("error", "Unknown error")
                } for record in failed_records]
            )
            failed_df.write.mode("append").option("mergeSchema", "true") \
                .saveAsTable("default.salesforce_failed_records", path="/user/hive/warehouse/salesforce_failed_records")
        
        logger.info("Results logged to Delta tables")
        
    except Exception as e:
        logger.warning(f"Failed to log results to Delta: {str(e)}")

# COMMAND ----------

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution flow
    """
    job_id = spark.sparkContext.getLocalProperty("spark.databricks.job.id") or "manual-run"
    
    try:
        logger.info("=" * 80)
        logger.info(f"Starting Salesforce sync job: {job_id}")
        logger.info("=" * 80)
        
        # Step 1: Authenticate with Salesforce
        logger.info("Step 1: Authenticating with Salesforce...")
        token = get_salesforce_token(
            SALESFORCE_CLIENT_ID,
            SALESFORCE_CLIENT_SECRET,
            SALESFORCE_USERNAME,
            SALESFORCE_PASSWORD
        )
        
        instance_url = get_instance_url(token)
        logger.info(f"Instance URL: {instance_url}")
        
        # Step 2: Read source data
        logger.info("Step 2: Reading source data from Unity Catalog...")
        df = read_source_data()
        
        if len(df) == 0:
            logger.warning("No records found in source table")
            return
        
        # Step 3: Transform data
        logger.info("Step 3: Transforming data...")
        transformed_df = transform_data(df)
        
        if len(transformed_df) == 0:
            logger.warning("No records after transformation")
            return
        
        # Step 4: Create Bulk API job and upload all data at once
        logger.info(f"Step 4: Creating Bulk API job and uploading {len(transformed_df)} records...")
        
        job_id = create_bulk_job(token, instance_url)
        upload_bulk_data(token, instance_url, job_id, transformed_df)
        close_bulk_job(token, instance_url, job_id)
        
        # Step 4b: Wait for job completion
        logger.info("Step 4b: Waiting for Bulk API job to complete...")
        job_state, job_info = wait_for_job_completion(token, instance_url, job_id)
        
        total_successful = job_info.get("numberSuccessful", 0)
        total_failed = job_info.get("numberFailed", 0)
        
        logger.info(f"Job {job_id} completed with state: {job_state}")
        
        # Step 4c: Retrieve failed records for logging
        logger.info("Step 4c: Retrieving failed record details...")
        all_failed_records = get_failed_records(token, instance_url, job_id)
        
        # Step 5: Log results
        logger.info("Step 5: Logging results...")
        log_results_to_delta(total_successful, total_failed, all_failed_records, job_id)
        
        # Summary
        logger.info("=" * 80)
        logger.info(f"Sync Complete")
        logger.info(f"Total Records Processed: {total_successful + total_failed}")
        logger.info(f"Successful: {total_successful}")
        logger.info(f"Failed: {total_failed}")
        logger.info(f"Success Rate: {round((total_successful / (total_successful + total_failed) * 100), 2)}%")
        logger.info("=" * 80)
        
        if total_failed > 0:
            dbutils.notebook.exit("PARTIAL_SUCCESS")
        else:
            dbutils.notebook.exit("SUCCESS")
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}", exc_info=True)
        dbutils.notebook.exit(f"FAILED: {str(e)}")

# COMMAND ----------

# Execute main function
main()
