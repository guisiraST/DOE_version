import pandas as pd
import numpy as np
import datetime
from datetime import datetime, timedelta

#Test ID 01
def check_inform_exit(data: pd.DataFrame, db: pd.DataFrame) -> tuple:
    """
    Check if a group of aliens has reported their departure from the company.

    Parameters:
    - data (pd.DataFrame): Current data.
    - db (pd.DataFrame): Historical data.

    Returns:
    - tuple: A tuple containing a string indicating overall status ('normal' or 'abnormal'),
             an integer count of abnormal entries, a list of ALIEN_IDs marked as 'normal',
             and a list of ALIEN_IDs marked as 'abnormal'.
    """
    abnormal_message =  "Aliens have not yet reported their departure from the old company."       

    data_filtered = data[data["MASTER_FORM_TYPE"] == "MT_59"]
    data_filtered_alien_id = data_filtered["ALIEN_ID"].to_list()
    db_filtered = db[db["ALIEN_ID"].isin(data_filtered_alien_id)]
    db_filtered['status'] = db_filtered['MASTER_FORM_TYPE'].apply(lambda x: 'normal' if 'MT_13_EXIT' in x else 'abnormal')

    data_filtered = pd.merge(data_filtered, db_filtered[['ALIEN_ID', 'status']], on='ALIEN_ID', how='left')
    data_filtered_abnormal = data_filtered[data_filtered["status"] == 'abnormal']
    data_filtered_abnormal['abnormal_desc'] = abnormal_message

    count_abnormal = data_filtered_abnormal.shape[0]
    result = 'normal' if count_abnormal < data_filtered.shape[0] else 'abnormal'

    return {'result': result,'count_abnormal': count_abnormal, 'data': data_filtered_abnormal}
    
    
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
    
    abnormal_message =  "The employer hires more than 10 aliens for different types of work and positions."       
    # Filter data based on form_id and master_form_type
    data_case = data[data['MASTER_FORM_TYPE'] == 'MT_59']
    
    # Group by relevant columns and count the number of aliens per job
    # data_case = data_case.groupby(['EMPLOYER_NO', 'FORM_ID', 'JOB_DESCRIPTION']).agg(ALIEN_COUNT=('ALIEN_ID', 'count')).reset_index()
    
    count_abnormal = 0
    job_abnormal_list = []
    
    #all_jobs = [job_config['job'] for job_config in config_case]
    specific_jobs = [job_config['job'] for job_config in config_case if job_config['job'] != "N/A"]
    na_jobs = data_case[~data_case['JOB_DESCRIPTION'].isin(specific_jobs)]['JOB_DESCRIPTION'].unique()
    
    # Check each job configuration
    for job_config in config_case:
        job = job_config['job']
        expected_number = job_config['number']
        # Calculate the total count of aliens for the specified job
        if job == "N/A":
            # Calculate the sum of ALIEN_ID count for jobs not listed in specific_jobs
            job_count = data_case[~data_case['JOB_DESCRIPTION'].isin(specific_jobs)]['ALIEN_ID'].count()
        else:
            job_count = data_case[data_case['JOB_DESCRIPTION'] == job]['ALIEN_ID'].count()
            
        # If the job count exceeds the expected number, return 'abnormal'
        if job_count > expected_number:
            count_abnormal += 1
            job_abnormal_list.append(job)
            
    if "N/A" in job_abnormal_list:
        job_abnormal_list.extend(na_jobs)
    
    data["status"] = data["JOB_DESCRIPTION"].apply(lambda x: 'normal' if not any(item in x for item in job_abnormal_list) else 'abnormal')        
    data["abnormal_desc"] = data['status'].apply(lambda x: 'pass' if x == 'normal' else abnormal_message)
    
    
    # If all job counts are within limits, return 'normal'
    result = 'abnormal' if "normal" not in data["status"].to_list() else 'normal'
    count_abnormal = data[data["status"] == "abnormal"].shape[0]
    return {'result': result,'count_abnormal': count_abnormal, 'job_abnormal': job_abnormal_list, 'data': data}


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
    
    # Calculate the condition
    merged_id['is_abnormal'] = (merged_id['CREATED_TIMESTAMP_x'] + pd.Timedelta(days=30)) > merged_id['VALID_UNTIL_y']
    
    aliens_abnormal_list = merged_id[merged_id['is_abnormal'] == True]["ALIEN_ID"]
    count_abnormal = len(aliens_abnormal_list)
    data_abnormal = data[data["ALIEN_ID"].isin(aliens_abnormal_list)]
    data_abnormal["status"] = "abnormal"
    data_abnormal["abnormal_desc"] = "The application submission date and the expiration date of the work permit is less than or equal to 30 days."
    
    # Determine anomaly status
    result = 'normal' if (len(merged_id["is_abnormal"]) - count_abnormal) > 0 else 'abnormal'
    return {"result":result, "count_abnormal": count_abnormal, "data": data_abnormal} 


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
    # Ensure CREATED_TIMESTAMP is in datetime format
    # db['CREATED_TIMESTAMP'] = pd.to_datetime(db['CREATED_TIMESTAMP'])

    # Get the index of the latest entry for each ALIEN_ID
    latest_indices = db.loc[db.groupby('ALIEN_ID')['CREATED_TIMESTAMP'].idxmax()]

    # Filter based on MASTER_FORM_TYPE
    ms_type_check = latest_indices[latest_indices['MASTER_FORM_TYPE'].isin(['MT_13_EXIT'])]
    ms_type_check['status_check'] = ms_type_check['MASTER_FORM_TYPE'].apply(lambda x: 'normal' if x == 'MT_13_EXIT' else 'abnormal')
    
    count_abnormal = (ms_type_check['status_check'] == 'abnormal').sum()
    result = 'abnormal' if count_abnormal > 0 else 'normal'
    
    return {"result":result, "count_abnormal": count_abnormal} 


#Test ID 16-21
def check_relocate_condition_from_B(data, df, limit_count, limit_days):
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
    count_abnormal = 0
    df.sort_values(by = "CREATED_TIMESTAMP", ascending= False, inplace= True)
    end_date = df.iloc[0]['CREATED_TIMESTAMP']
    # Ensure CREATED_TIMESTAMP is in datetime format
    df['CREATED_TIMESTAMP'] = pd.to_datetime(df['CREATED_TIMESTAMP'])

    # If end_date is a Timestamp, convert it to datetime; otherwise, parse it
    if isinstance(end_date, pd.Timestamp) or isinstance(end_date, pd.DatetimeIndex):
        end_date = end_date.to_pydatetime()
    else:
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Calculate the start date for the two-month timeframe
    start_date_time_frame = end_date - timedelta(days=90)
    # Filter the DataFrame for the two-month timeframe
    df_time_frame = df[(df['CREATED_TIMESTAMP'] >= start_date_time_frame) & (df['CREATED_TIMESTAMP'] <= end_date)]
    # Calculate the total count within the two-month timeframe
    total_count_time_frame = df_time_frame['ALIEN_COUNT'].sum()

    # Check if the total count within the two-month timeframe exceeds the limit_count
    if total_count_time_frame > limit_count:
        # Calculate the start date by subtracting limit_days from end date
        start_date = end_date - timedelta(days=limit_days - 1)
        
        # Filter the DataFrame between the calculated start date and the end date
        filtered_df = df[(df['CREATED_TIMESTAMP'] >= start_date) & (df['CREATED_TIMESTAMP'] <= end_date)]
        
        # Calculate total count and number of days within the limit_days timeframe
        total_count = filtered_df['ALIEN_COUNT'].sum()
        total_days = (filtered_df['CREATED_TIMESTAMP'].max() - filtered_df['CREATED_TIMESTAMP'].min()).days + 1
        # Check scenarios
        if total_count < limit_count:
            data["status"] = "normal"
            data["abnormal_desc"] = "pass"
        else:
            if total_days <= limit_days:
                data["status"] = "abnormal"
                data["abnormal_desc"] = "Aliens moved to B exceeding the limit of people and have been relocated for more than a specified number of days."
                count_abnormal = total_count
            else:
                data["status"] = "normal"
                data["abnormal_desc"] = "pass"
    else:
        data["status"] = "normal"
        data["abnormal_desc"] = "pass"
        
    result = 'normal' if count_abnormal < data.shape[0] else 'abnormal'    
    total_relocate_day = "pass" if result == "normal" else total_days
    return {'result': result,'count_abnormal': count_abnormal, 'total_relocate_day': total_relocate_day, 'data': data}
    
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
    result = check_relocate_condition_from_B(data, count_alien_date, config_case['number'], config_case['day'])
    return result