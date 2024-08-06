import sys
import os
from .module import *


def flow_4(data, db, config_case):
    """
    Flow 4: Evaluate relocation conditions and departure reporting status of aliens.

    Parameters:
    - data (pd.DataFrame): Current dataset containing relocation requests.
    - db (pd.DataFrame): Historical dataset of relocation records.
    - config_case (dict): Configuration specifying the limit on the number of aliens and maximum days for relocation.
        - "number" (int): Maximum allowed number for aliens.
        - "day" (str): limit number move to B
      Example:
        config_case = {
            "number": 50,
            "day": 20
        }

    Returns:
    - dict: A dictionary with the following keys:
        - 'status' (str): Overall status ("normal" or "abnormal").
        - 'message' (str): Description of the status.
        - 'count_abnormal' (int): Count of abnormalities found.
        - 'total_relocate_day' (int): Total days of relocation.
        - 'data' (list of dict): Relevant data for further inspection.
    """
    messages = {
        "normal": "Aliens moved to B not exceeding the limit of people and have been relocated for less than a specified number of days.",
        "abnormal_relocate_B": "Aliens moved to B exceeding the limit of people and have been relocated for more than a specified number of days.",
        "check_inform_exit": "Aliens have not yet reported their departure from the old company but have already applied for the new one."
    }
    
    # Check if aliens have reported their departure
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
        
        # Create concat_df
        data_case_agg = data_case.groupby(['CREATED_TIMESTAMP', 'EMPLOYER_NO', 'FORM_ID']).agg(ALIEN_COUNT=('ALIEN_ID', 'count')).reset_index()
        db_case_agg = db_case.groupby(['CREATED_TIMESTAMP', 'EMPLOYER_NO', 'FORM_ID']).agg(ALIEN_COUNT=('ALIEN_ID', 'count')).reset_index()
        concat_df = pd.concat([db_case_agg, data_case_agg], axis=0).reset_index(drop=True)
        
        result_check_relocate_condition_from_B = check_relocate_condition_from_B(data_case, concat_df, config_case["number"], config_case["day"])
        
        if result_check_relocate_condition_from_B["result"] == "normal":
            result.update({
                "status": "normal", 
                "message": messages["normal"], 
                "count_abnormal": result_check_relocate_condition_from_B['count_abnormal'],
                "total_relocate_day": result_check_relocate_condition_from_B['total_relocate_day']
            })
            combined_data = pd.concat([result_check_inform_exit["data"], result_check_relocate_condition_from_B["data"]], ignore_index=True)
            combined_data["case_code"] = combined_data['status'].apply(lambda x: 'pass' if x == 'normal' else "R4/2")
            result['data'] = combined_data.to_dict(orient='records')
        else:
            result.update({
                "status": "abnormal", 
                "message": messages["abnormal_relocate_B"],
                "count_abnormal": result_check_relocate_condition_from_B['count_abnormal'],
                "total_relocate_day": result_check_relocate_condition_from_B['total_relocate_day']
            })
            result_check_relocate_condition_from_B["data"]["case_code"] = "R4/2"
            result_check_inform_exit["data"]["case_code"] = "R4/1"
            combined_data = pd.concat([result_check_inform_exit["data"], result_check_relocate_condition_from_B["data"]], ignore_index=True)
            result['data'] = combined_data.to_dict(orient='records')
    
    else:
        result.update({
            "status": "abnormal", 
            "message": messages["check_inform_exit"],
            "count_abnormal": result_check_inform_exit['count_abnormal'],
            "total_relocate_day" : None
        })
        result_check_inform_exit["data"]["case_code"] = "R4/1"
        result['data'] = result_check_inform_exit['data'].to_dict(orient='records')
    
    return result


   