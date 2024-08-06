import pandas as pd
import numpy as np
import datetime
from datetime import datetime, timedelta

#Test ID 01
def check_inform_exit(data: pd.DataFrame, db: pd.DataFrame) -> dict:
    """
    Check A group of aliens has not yet reported their departure from the company.
    
    Parameters:
    - data (pd.DataFrame): Current data.
    - db (pd.DataFrame): Historical data.
    
    Returns:
    - dict: A dictionary with ALIEN_ID as keys and a boolean indicating if 'MT_13_EXIT' is present in the latest record.
    """
    results = {}

    # Create a set of unique ALIEN_IDs for efficiency
    alien_ids = set(data["ALIEN_ID"])

    for alien_id in alien_ids:
        # Check if the status register is MT_59
        if "MT_59" in data[data["ALIEN_ID"] == alien_id]["MASTER_FORM_TYPE"].values:
            # Filter and sort the db for the given ALIEN_ID
            db_filtered = db[db["ALIEN_ID"] == alien_id].sort_values(by="CREATED_TIMESTAMP", ascending=False)
            
            if not db_filtered.empty:
                # Get the latest record
                latest_record = db_filtered.iloc[0]
                # Check for 'MT_13_EXIT' in 'MASTER_FORM_TYPE'
                check_anomaly = 'MT_13_EXIT' in latest_record['MASTER_FORM_TYPE']
                results[alien_id] = check_anomaly

                # print(f'ALIEN_ID {alien_id}: {"normal" if check_anomaly else "abnormal"}')
            else:
                # print(f'ALIEN_ID {alien_id}: No records found')
                results[alien_id] = None  # Indicates no data found for this ALIEN_ID
        else:
            # print(f'ALIEN_ID {alien_id}: Master form type not correct')
            results[alien_id] = False  # Indicates the master form type is not correct

    return 'abnormal' if False in results.values() else 'normal'
    
    
#Test ID 02-05
def check_job_limits(data, config_case):
    """
    Check an employer hires more than 10 aliens for different types of work and positions.

    Parameters:
    - data (pd.DataFrame): The input data containing job information.
    - config_case (list of dict): A list of dictionaries containing job configurations with 'job' and 'number' keys.

    Returns:
    - str: 'normal' if all job counts are within limits, 'abnormal' otherwise.
    """
    # Filter data based on form_id and master_form_type
    data_case = data[data['MASTER_FORM_TYPE'] == 'MT_59']
    
    # Group by relevant columns and count the number of aliens per job
    data_case = data_case.groupby(['EMPLOYER_NO', 'FORM_ID', 'JOB_DESCRIPTION']).agg(ALIEN_COUNT=('ALIEN_ID', 'count')).reset_index()
    
    # Check each job configuration
    for job_config in config_case:
        job = job_config['job']
        expected_number = job_config['number']
        
        # Calculate the total count of aliens for the specified job
        job_count = data_case[data_case['JOB_DESCRIPTION'] == job]['ALIEN_COUNT'].sum()
        # print(f'job: {job}, expected: {expected_number}, job_count: {job_count}')
        
        # If the job count exceeds the expected number, return 'abnormal'
        if job_count > expected_number:
            return 'abnormal'
    
    # If all job counts are within limits, return 'normal'
    return 'normal'


#Test ID 6
def check_expire_condition(data, db):
    """
    remaining period between the application submission date and the expiration date of the work permit is less than or equal to 30 days.

    Parameters:
    - data (pd.DataFrame): The input data containing job information.
    - db (pd.DataFrame): The database containing exit information.

    Returns:
    - str: 'normal' if the timestamp condition is met, 'abnormal' otherwise.
    """
    # Filter data based on master_form_type
    data_case = data[data['MASTER_FORM_TYPE'] == 'MT_59']
    db_case = db[db['MASTER_FORM_TYPE'] == 'MT_13_EXIT']
    
    # Merge data and db on 'ALIEN_ID'
    merged_id = pd.merge(data_case, db_case, on='ALIEN_ID', how='inner')
    
    # Convert timestamps to datetime
    merged_id['CREATED_TIMESTAMP_x'] = pd.to_datetime(merged_id['CREATED_TIMESTAMP_x'])
    merged_id['VALID_UNTIL_y'] = pd.to_datetime(merged_id['VALID_UNTIL_y'])
    
    # Check timestamp condition
    check_anomaly = (merged_id['CREATED_TIMESTAMP_x'] + pd.Timedelta(days=30)) < merged_id['VALID_UNTIL_y']
    
    # Determine anomaly status
    return 'abnormal' if not check_anomaly.iloc[0] else 'normal'


#Test ID 7 **remark similar test ID 1 instead
def check_status_resign_a(data, db):
    """
    Check aliens has not yet reported their departure from the old company but has already applied for the new one.

    Parameters:
    - data (pd.DataFrame): The input data containing job information.
    - db (pd.DataFrame): The database containing status information.

    Returns:
    - str: 'normal' if the required status is present, 'abnormal' otherwise.
    """
    # Filter data based on master_form_type
    data_case = data[data['MASTER_FORM_TYPE'] == 'MT_59']
    
    # Merge data and db on 'ALIEN_ID'
    merged_id = pd.merge(data_case, db, on='ALIEN_ID', how='inner',suffixes=('_A', '_B'))
    
    # Check if the required status is present in the merged data
    return 'abnormal' if 'MT_13_EXIT' not in merged_id['MASTER_FORM_TYPE_B'].values else 'normal'


#Test ID 8 ***
def check_status_resign_b(db):
    """
    Check aliens has moved out of the company but has not yet reported their arrival at the new place.

    Parameters:
    - db (pd.DataFrame): The database containing status information.

    Returns:
    - str: 'normal' if the required status is not present, 'abnormal' otherwise.
    """
    # Check if the required status is present in the db
    return 'normal' if "MT_13_EXIT" not in db['MASTER_FORM_TYPE'].values else 'abnormal'


#Test ID 16-21
def check_relocate_condition_from_B(concat_df, limit_count, limit_days):
    """
    Check if a group of aliens moved to location B within the limit of people and have been relocated for more than a specified number of days. 

    Parameters:
    - df (pd.DataFrame): DataFrame containing relocation data with 'CREATED_TIMESTAMP' and 'ALIEN_COUNT' columns.
    - end_date (str or pd.Timestamp): The end date for the check in 'YYYY-MM-DD' format or as a Timestamp.
    - limit_count (int): The limit of people allowed to relocate.
    - limit_days (int): The number of days to check for relocation.

    Returns:
    - str: A message indicating whether the condition is normal or abnormal.
    """
    concat_df.sort_values(by = "CREATED_TIMESTAMP", ascending= False, inplace= True)
    end_date = concat_df.iloc[0]['CREATED_TIMESTAMP']
    # Ensure CREATED_TIMESTAMP is in datetime format
    concat_df['CREATED_TIMESTAMP'] = pd.to_datetime(concat_df['CREATED_TIMESTAMP'])

    # If end_date is a Timestamp, convert it to datetime; otherwise, parse it
    if isinstance(end_date, pd.Timestamp) or isinstance(end_date, pd.DatetimeIndex):
        end_date = end_date.to_pydatetime()
    else:
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Calculate the start date for the two-month timeframe
    start_date_time_frame = end_date - timedelta(days=90)
    # Filter the DataFrame for the two-month timeframe
    df_time_frame = concat_df[(concat_df['CREATED_TIMESTAMP'] >= start_date_time_frame) & (concat_df['CREATED_TIMESTAMP'] <= end_date)]
    # Calculate the total count within the two-month timeframe
    total_count_time_frame = df_time_frame['ALIEN_COUNT'].sum()

    # Check if the total count within the two-month timeframe exceeds the limit_count
    if total_count_time_frame > limit_count:
        # Calculate the start date by subtracting limit_days from end date
        start_date = end_date - timedelta(days=limit_days - 1)
        
        # Filter the DataFrame between the calculated start date and the end date
        filtered_df = concat_df[(concat_df['CREATED_TIMESTAMP'] >= start_date) & (concat_df['CREATED_TIMESTAMP'] <= end_date)]
        
        # Calculate total count and number of days within the limit_days timeframe
        total_count = filtered_df['ALIEN_COUNT'].sum()
        total_days = (filtered_df['CREATED_TIMESTAMP'].max() - filtered_df['CREATED_TIMESTAMP'].min()).days + 1
        # Check scenarios
        if total_count < limit_count:
            return "normal"
        else:
            if total_days <= limit_days:
                return "abnormal"
            else:
                return "normal"
    else:
        return "normal"
    
#Test ID 9-14
def check_relocate_condition_from_A_to_B(data, db, config_case, EMPLOYER_NO_A, EMPLOYER_NO_B):
    """
    Check if a group of aliens moved from A to B exceeding the limit of people and have been relocated for more than a specified number of days.

    Parameters:
    - data (pd.DataFrame): DataFrame containing access preparation data.
    - db (pd.DataFrame): DataFrame containing exit preparation data.
    - config_case (dict): Configuration case containing 'number' and 'day' keys.
    - EMPLOYER_NO_A (str): Employer number for location A.
    - EMPLOYER_NO_B (str): Employer number for location B.

    Returns:
    - tuple: A tuple containing the anomaly status and the merged DataFrame.
    """
    # Filter for MT_13_EXIT at location A
    db_exit_filter = db[(db["EMPLOYER_NO"] == EMPLOYER_NO_A) & (db["MASTER_FORM_TYPE"] == "MT_13_EXIT")]

    # Filter for MT_59 at location B
    db_access_filter = data[(data["EMPLOYER_NO"] == EMPLOYER_NO_B) & (data["MASTER_FORM_TYPE"] == "MT_59")]

    # Merge on ALIEN_ID
    merged_alien_id = pd.merge(db_exit_filter, db_access_filter, on='ALIEN_ID', how='inner', suffixes=('_A', '_B'))

    # Group by date and employer numbers
    count_alien_date = merged_alien_id.groupby(['CREATED_TIMESTAMP_A', "EMPLOYER_NO_A", "EMPLOYER_NO_B"]).agg(
        ALIEN_COUNT=('ALIEN_ID', 'count')
    ).reset_index().rename(columns={'CREATED_TIMESTAMP_A': 'CREATED_TIMESTAMP'})
        
    # Check relocation condition
    anomaly = check_relocate_condition_from_B(
        count_alien_date, 
        config_case['number'], 
        config_case['day']
    )
    
    return anomaly