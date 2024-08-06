import pandas as pd
# from flow import *
from flow.prep_data import *

from flow.flow1 import flow_1
from flow.flow2 import flow_2
from flow.flow4 import flow_4


flow_id_config = {
    "flow1" : 
        {"TC01" : ["00001"],
        "TC02" : ["00002"],
        "TC03" : ["00003"],
        "TC04" : ["00004"],
        "TC05" : ["00005"]},
    "flow2" : 
        {"TC06" : ["00006"],
        "TC07" : ["00007"],
        # "TC08" : ["00008"],
        "TC09" : ["00009","00010"],
        "TC10" : ["00011","00012"],
        "TC11" : ["00013","00014"],
        "TC12" : ["00015","00016"],
        "TC13" : ["00017","00018"],
        "TC14" : ["00019","00020"],
        },
    "flow4" :
        {"TC15" : ["00021"],
         "TC16" : ["00022","00023","00024"],
         "TC17" : ["00025","00026"],
         "TC18" : ["00027","00028"],
         "TC19" : ["00029","00030","00031"],
         "TC20" : ["00032","00033","00034"],
         "TC21" : ["00035","00036","00037"],
        }
}

df_data = pd.read_excel("/opt/project/src/DOE/src/function/Testcase_DOE_2-7-2024.xlsx", sheet_name="Test_Case")
df_db = pd.read_excel("/opt/project/src/DOE/src/function/Testcase_DOE_2-7-2024.xlsx", sheet_name="Prerequisite")
data = prep_data(df_data)
db = prep_data(df_db)

selected_cols = [
    'CREATED_TIMESTAMP',
    'FORM_ID',
    'ALIEN_ID',
    'JOB_DESCRIPTION',
    'EMPLOYER_NO',
    'MASTER_FORM_TYPE',
    'MASTER_FORM_STATUS',
    'VALID_UNTIL'
    ]
db = db[selected_cols]
data = data[selected_cols]

def test_case(data, db):
    result_list = []
    for flow_key, tc_dict in flow_id_config.items():
        # print(f"\nFlow: {flow_key}")
        for tc_key, tc_list in tc_dict.items():
            # print(f"  {tc_key}: {tc_list}")
            
            data_case = data[data["FORM_ID"].isin(tc_list)]
            db_case = db[db["FORM_ID"].isin(tc_list)]
            
            if flow_key == "flow1":
                config_case = [
                    {"job": "กรรมกร", "number": 10},
                    {"job": "งานขายของหน้าร้าน", "number": 10},
                    {"job": "งานทํามือ", "number": 10},
                    {"job": "N/A", "number": 10}
                ]
                result = flow_1(data_case, db_case, config_case)
                # result["tc"] = tc_key 
                # result["form_id"] = tc_list
                result_list.append(result)
            
                
            if flow_key == "flow2":
                config_case = {
                "number": 20,
                "day": 14
                }
                EMPLOYER_NO_A = db_case["EMPLOYER_NO"].to_list()[0]
                EMPLOYER_NO_B = data_case["EMPLOYER_NO"].to_list()[0]
                result = flow_2(data_case, db_case, config_case, EMPLOYER_NO_A, EMPLOYER_NO_B)
                # result["tc"] = tc_key 
                # result["form_id"] = tc_list
                result_list.append(result)
                
            if flow_key == "flow4":
                config_case = {
                "number": 50,
                "day": 20
                }
                result = flow_4(data_case, db_case, config_case)
                # result["tc"] = tc_key 
                # result["form_id"] = tc_list
                result_list.append(result)
    
    return result_list     
        
result_list = test_case(data, db)
for i in result_list:
    print("\n",i)
# df_result = pd.DataFrame(result_list)
# df_result.to_csv("result_test_script.csv", index=False)