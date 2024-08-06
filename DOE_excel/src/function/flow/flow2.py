import sys
import os
from .module import *

from flow.module import *

def flow_2(data, db, config_case, EMPLOYER_NO_A, EMPLOYER_NO_B):
    """
    Flow 2: Validate the movement of aliens from employer A to employer B based on various conditions.

    Parameters:
    - data (pd.DataFrame): DataFrame containing current alien movement data.
    - db (pd.DataFrame): DataFrame containing historical alien movement data.
    - config_case (dict): Dictionary containing 'number' and 'day' for configuration.
        - "number" (int): Maximum allowed number for aliens.
        - "day" (str): limit number move from A to B
        Example:
            config_case = {
                "number": 20,
                "day": 14
            }
    - EMPLOYER_NO_A (str): Employer number for location A.
    - EMPLOYER_NO_B (str): Employer number for location B.

    Returns:
    - dict: A dictionary with the following keys:
        - 'status' (str): Overall status of the check ('normal' or 'abnormal').
        - 'message' (str): Descriptive message about the check result.
        - 'count_abnormal' (int): Count of abnormal records.
        - 'total_relocate_day' (int): Total days aliens have been relocated.
        - 'data' (list of dict): Relevant data records.
    """
    # Define messages
    messages = {
        "normal": "Aliens moved from A to B not exceeding the limit of people and have been relocated for less than a specified number of days.",
        "abnormal_relocate": "Aliens moved from A to B exceeding the limit of people and have been relocated for more than a specified number of days.",
        "abnormal_resign_B": "Aliens has moved out of the company but has not yet reported.",
        "abnormal_resign_A": "Aliens has not yet reported their departure from the old company but has already applied for the new one.",
        "abnormal_expire": "The application submission date and the expiration date of the work permit is less than or equal to 30 days."
    }

    # Check conditions in sequence
    result_check_inform_exit = check_inform_exit(data, db)
    
    # Initialize result dictionary
    result = {
        'status': '',
        'message': '',
        'count_abnormal': 0,
        'total_relocate_day': 0,
        'data': None
    }
    
    if result_check_inform_exit["result"] == "normal": 
        
        alien_ids_to_drop = result_check_inform_exit["data"]['ALIEN_ID'].unique()
        data_case = data[~data['ALIEN_ID'].isin(alien_ids_to_drop)]
        db_case = db[~db['ALIEN_ID'].isin(alien_ids_to_drop)]
        
        result_check_expire_condition = check_expire_condition(data_case, db_case) 
        
        if result_check_expire_condition["result"] == "normal":
            alien_ids_to_drop_2 = result_check_expire_condition["data"]['ALIEN_ID'].unique()
            data_case_2 = data_case[~data_case['ALIEN_ID'].isin(alien_ids_to_drop_2)]
            db_case_2 = db_case[~db_case['ALIEN_ID'].isin(alien_ids_to_drop_2)]
            
            result_check_relocate_condition_from_A_to_B = check_relocate_condition_from_A_to_B(data_case_2,
                                                                                                    db_case_2, 
                                                                                                    config_case, 
                                                                                                    EMPLOYER_NO_A, 
                                                                                                    EMPLOYER_NO_B)
            
            if result_check_relocate_condition_from_A_to_B["result"] == "normal":
                result.update({
                    "status": "normal", 
                    "message": messages["normal"],
                    "count_abnormal": result_check_relocate_condition_from_A_to_B['count_abnormal'],
                    "total_relocate_day": result_check_relocate_condition_from_A_to_B['total_relocate_day']
                })
                combined_data = pd.concat([result_check_inform_exit["data"], result_check_expire_condition["data"], result_check_relocate_condition_from_A_to_B["data"]], ignore_index=True)
                combined_data["case_code"] = combined_data['status'].apply(lambda x: 'pass' if x == 'normal' else "R2/3")
                result['data'] = combined_data.to_dict(orient="records")
            else:
                result.update({
                    "status": "abnormal", 
                    "message": messages["abnormal_relocate"],
                    "count_abnormal": result_check_relocate_condition_from_A_to_B['count_abnormal'],
                    "total_relocate_day": result_check_relocate_condition_from_A_to_B['total_relocate_day']
                })
                result_check_relocate_condition_from_A_to_B["data"]["case_code"] = "R2/3"
                result_check_expire_condition["data"]["case_code"] = "R2/2"
                result_check_inform_exit["data"]["case_code"] = "R1/2"
                combined_data = pd.concat([result_check_inform_exit["data"], result_check_expire_condition["data"], result_check_relocate_condition_from_A_to_B["data"]], ignore_index=True)
                result['data'] = combined_data.to_dict(orient="records")
        else:
            result.update({
                "status": "abnormal", 
                "message": messages["abnormal_expire"],
                "count_abnormal": result_check_expire_condition['count_abnormal'],
                "total_relocate_day": None
            })
            result_check_expire_condition["data"]["case_code"] = "R2/2"
            result_check_inform_exit["data"]["case_code"] = "R1/2"
            combined_data = pd.concat([result_check_inform_exit["data"], result_check_expire_condition["data"]], ignore_index=True)
            result['data'] = combined_data.to_dict(orient="records")
    else:
        result.update({
            "status": "abnormal",
            "message": messages["abnormal_resign_A"],
            "count_abnormal": result_check_inform_exit['count_abnormal'],
            "total_relocate_day": None
        })
        result_check_inform_exit["data"]["case_code"] = "R2/1"
        result['data'] = result_check_inform_exit["data"].to_dict(orient="records")
    
    return result
