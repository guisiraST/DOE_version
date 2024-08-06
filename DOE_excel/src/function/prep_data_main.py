import pandas as pd

def prep_data(df):

    #Rename columns
    df.rename(columns={
                'CREATED_TIMESTAMP\n(วันที่ยื่นคำขอ)': 'CREATED_TIMESTAMP',
                'FORM_ID\n(รหัสฟอร์ม)': 'FORM_ID',
                'FORM_ID_SEQ\n(ลำดับรหัสฟอร์ม)': 'FORM_ID_SEQ',
                'ALIEN_ID\n(รหัสคนต่างด้าว)': 'ALIEN_ID',
                'ALIEN_SEQ\n(ลำดับรหัสคนต่างด้าว)': 'ALIEN_SEQ',
                'EMPLOYER_ID\n(รหัสนายจ้าง)': 'EMPLOYER_ID',
                'COMPANYNAME_TH\n(ชื่อนายจ้างไทย/สถานประกอบการ)': 'COMPANYNAME_TH',
                'COMPANYNAME_EN\n(ชื่อบริษัท (อังกฤษ))': 'COMPANYNAME_EN',
                'BUS_TYPE_ID\n(รหัสประเภทกิจการ)': 'BUS_TYPE_ID',
                'Job Description\n(ตำแหน่งงานลูกจ้าง)': 'Job_Description',
                'EMPLOYER_NO\n(เลขปชช./นิติบุคคล)': 'EMPLOYER_NO',
                'Master_Form_Type\n(FORM_TYPE_ID)': 'Master_Form_Type',
                'Master_Form_Status\n(Tracking_Status)': 'Master_Form_Status',
                   }, inplace=True)
    df.columns = [x.upper() for x in df.columns]

    # Convert columns to datetime
    df['CREATED_TIMESTAMP'] = pd.to_datetime(df['CREATED_TIMESTAMP'])
    # df['CREATED_TIMESTAMP'] = df['CREATED_TIMESTAMP'].dt.date

    # Split and explode columns ALIEN_ID, ALIEN_SEQ, Job Description
    df['ALIEN_ID'] = df['ALIEN_ID'].str.split(',')
    df['ALIEN_SEQ'] = df['ALIEN_SEQ'].apply(lambda x: x.split(',') if isinstance(x, str) and ',' in x else [x])
    df['JOB_DESCRIPTION'] = df['JOB_DESCRIPTION'].str.split(',')

    # Explode the columns
    exploded_A = df.explode('ALIEN_ID').reset_index(drop=True)
    exploded_B = df.explode('ALIEN_SEQ').reset_index(drop=True)
    exploded_C = df.explode('JOB_DESCRIPTION').reset_index(drop=True)
    
    # Combine the exploded columns back into a single DataFrame
    exploded_df = exploded_A.copy()
    exploded_df['ALIEN_SEQ'] = exploded_B['ALIEN_SEQ']
    exploded_df['JOB_DESCRIPTION'] = exploded_C['JOB_DESCRIPTION']

    # Fill NaN values with previous values
    exploded_df = exploded_df.ffill()

    def remove_quote(x):
        return x.replace('"', '') if isinstance(x, str) else x

    # List of columns to process
    columns_to_process = [
        'ALIEN_ID', 'FORM_ID', 'EMPLOYER_ID', 
        'COMPANYNAME_TH', 'COMPANYNAME_EN', 
        'BUS_TYPE_ID', 'EMPLOYER_NO', 'JOB_DESCRIPTION'
    ]

    # Apply remove_quote function to each column in the list
    for column in columns_to_process:
        exploded_df[column] = exploded_df[column].apply(remove_quote)

    return exploded_df