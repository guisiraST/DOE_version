import sys
import os
from .module import *

def flow_1(data, db, config_case):
    """
    Flow 1: Check if aliens have reported their departure and if the employer hires within the job limits.

    Parameters:
    - data (pd.DataFrame): Current data.
    - db (pd.DataFrame): Historical data.
    - config_case (list of dict): Configuration specifying job limits, where each dict has:
        - "job" (str): Job title.
        - "number" (int): Maximum allowed number for this job.
    
    Example of `config_case`:
    config_case = [
        {"job": "กรรมกร", "number": 10},
        {"job": "งานขายของหน้าร้าน", "number": 10},
        {"job": "งานทํามือ", "number": 10},
        {"job": "N/A", "number": 10}
    ]

    Returns:
    - dict: A dictionary containing:
        - "status" (str): 'normal' or 'abnormal'.
        - "message" (str): Description of the check results.
        - "count_abnormal" (int): Number of abnormalities found.
        - "job_abnormal" (list of dict, optional): Details of job limit violations.
        - "data" (list of dict): Data of aliens relevant to the check.
    """
    messages = {
        "normal": "Aliens have reported their departure from the old company, and the employer hires not more than 10 aliens for different types of work and positions.",
        "abnormal_inform_exit": "Some aliens have not yet reported their departure from the old company.",
        "abnormal_job_limits": "Aliens have reported their departure from the old company. However, the employer hires more than 10 aliens for different types of work and positions.",
    }
    
    # Check if aliens have reported their departure
    result_check_inform_exit = check_inform_exit(data, db)
    
    # Initialize result dictionary
    result = {
        'status': '',
        'message': '',
        'count_abnormal': 0,
        'data': None,
        'job_abnormal': None  # Only included if job limits check is abnormal
    }
    
    if result_check_inform_exit["result"] == "normal":
        alien_ids_to_drop = result_check_inform_exit["data"]['ALIEN_ID'].unique()
        result_check_job_limits = check_job_limits(data[~data['ALIEN_ID'].isin(alien_ids_to_drop)], config_case)
        
        if result_check_job_limits["result"] == "normal":
            result.update({
                "status": "normal",
                "message": messages["normal"],
                "count_abnormal": result_check_job_limits["count_abnormal"],
                'job_abnormal': result_check_job_limits["job_abnormal"]
            })
            # Combine data from both checks into a DataFrame
            result_check_job_limits["data"]["case_code"] =  result_check_job_limits["data"]['status'].apply(lambda x: 'pass' if x == 'normal' else "R1/2")
            result_check_inform_exit["data"]["case_code"] = "R1/1"
            combined_data = pd.concat([result_check_inform_exit["data"], result_check_job_limits["data"]])
            combined_data = combined_data.reset_index(drop=True)
            result['data'] = combined_data.to_dict(orient='records')  # Convert DataFrame to dict
        else:
            result.update({
                "status": "abnormal",
                "message": messages["abnormal_job_limits"],
                "count_abnormal": result_check_job_limits["count_abnormal"],
                'job_abnormal': result_check_job_limits["job_abnormal"]
            })
            # Correct status in data from inform exit check
            result_check_job_limits["data"]['status'] = 'abnormal'
            result_check_job_limits["data"]["case_code"] =  result_check_job_limits["data"]['status'].apply(lambda x: 'pass' if x == 'normal' else "R1/2")
            result_check_inform_exit["data"]["case_code"] = "R1/1"

            combined_data = pd.concat([result_check_inform_exit["data"], result_check_job_limits["data"]], ignore_index=True)
            result['data'] = combined_data.to_dict(orient="records")
    
    else:
        result.update({
            "status": "abnormal",
            "message": messages["abnormal_inform_exit"],
            "count_abnormal": result_check_inform_exit['count_abnormal'],
            'job_abnormal': None
        })
        result_check_inform_exit["data"]["case_code"] = "R1/1"
        result['data'] = result_check_inform_exit["data"].to_dict(orient='records')  # Convert DataFrame to dict
    
    return result