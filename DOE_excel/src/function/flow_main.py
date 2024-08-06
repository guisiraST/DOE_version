from DOE.src.function.module_main import *

def flow_1(data, db, config_case):
    """
    Flow 1: Check if aliens have reported their departure and if the employer hires within the job limits.

    Parameters:
    - data (pd.DataFrame): Current data.
    - db (pd.DataFrame): Historical data.
    - config_case (list of dict): Configuration case containing job limits.

    Returns:
    - dict: A dictionary containing the status and message of the check.
    """
    # Check if aliens have reported their departure
    result_check_inform_exit = check_inform_exit(data, db)
    
    if result_check_inform_exit == "normal":
        # Check if the employer hires within the job limits
        result_check_job_limits = check_job_limits(data, config_case)
        
        if result_check_job_limits == "normal":
            return {
                "status": "normal",
                "message": "Aliens have reported their departure from the old company, and the employer hires not more than 10 aliens for different types of work and positions."
            }
        else:
            return {
                "status": "abnormal",
                "message": "Aliens have reported their departure from the old company. However, the employer hires more than 10 aliens for different types of work and positions."
            }
    else:
        return {
            "status": "abnormal",
            "message": "Some aliens have not yet reported their departure from the old company."
        }
        
def flow_2(data, db, config_case, EMPLOYER_NO_A, EMPLOYER_NO_B):
    """
    Flow 2: Check various conditions for aliens moving from employer A to employer B.

    Parameters:
    - data (pd.DataFrame): Current data.
    - db (pd.DataFrame): Historical data.
    - config_case (dict): Configuration case containing 'number' and 'day' keys.
    - EMPLOYER_NO_A (str): Employer number for location A.
    - EMPLOYER_NO_B (str): Employer number for location B.

    Returns:
    - dict: A dictionary containing the status and message of the check.
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
    if check_status_resign_a(data, db) != "normal": 
        return {"status": "abnormal", "message": messages["abnormal_resign_A"]}
    
    # if check_status_resign_b(db) != "normal":
    #     return {"status": "abnormal", "message": messages["abnormal_resign_B"]}
    
    if check_relocate_condition_from_A_to_B(data, db, config_case, EMPLOYER_NO_A, EMPLOYER_NO_B) != "normal":
        return {"status": "abnormal", "message": messages["abnormal_relocate"]}
    
    if check_expire_condition(data, db) == "normal":
        return {"status": "normal", "message": messages["normal"]}
    
    return {"status": "abnormal", "message": messages["abnormal_expire"]}

def flow_4(data, db, concat_df, config_case):
    
    messages = {
        "normal": "Aliens moved to B not exceeding the limit of people and have been relocated for less than a specified number of days.",
        "abnormal_relocate_B": "Aliens moved to B exceeding the limit of people and have been relocated for more than a specified number of days.",
        "abnormal_resign_A": "Aliens has not yet reported their departure from the old company but has already applied for the new one."
    }
    result_check_status_resign_a = check_status_resign_a(data, db)
    if result_check_status_resign_a == "normal":   
        result_check_relocate_condition_from_B = check_relocate_condition_from_B(concat_df, config_case["number"], config_case["day"])
        if result_check_relocate_condition_from_B == "normal":
            return {"status": "normal", "message": messages["normal"]}
        else:
            return {"status": "abnormal", "message": messages["abnormal_relocate_B"]}
    else:
       return {"status": "abnormal", "message": messages["abnormal_resign_A"]}