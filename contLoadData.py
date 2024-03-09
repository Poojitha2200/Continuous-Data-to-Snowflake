import os
import snowflake.connector
from snowflake.connector import DictCursor

# Snowflake connection parameters
snowflake_user = '****'
snowflake_password = '*****'
snowflake_account = '*****'
snowflake_warehouse = 'COMPUTE_WH'
snowflake_database = 'local'
snowflake_schema = 'PUBLIC'
snowflake_stage = '*****_Stage'

# Local folder path to monitor for new files
local_folder_path = 'C:/Users/*****'

# Snowflake connection
conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema
)
print('Connection established Successfully')


# Function to upload file to Snowflake stage
def upload_to_snowflake_stage(file_path, stage_name):
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(f'PUT file://{file_path} @{stage_name}')
        print(f'Successfully uploaded {file_path} to Snowflake stage {stage_name}')
        # cursor.execute(f''' COPY INTO LOCAL.PUBLIC.LOCATIONS FROM @LOCAL.PUBLIC.LOCAL_STAGE ;''')
        # print(f'Successfully uploaded')
    except snowflake.connector.errors.ProgrammingError as e:
        print(f'Error uploading {file_path} to Snowflake stage {stage_name}: {e}')
    finally:
        cursor.close()

# Monitor the local folder for new files and upload to Snowflake stage
while True:
    files = [f for f in os.listdir(local_folder_path) if os.path.isfile(os.path.join(local_folder_path, f))]
    for file in files:
        file_path = os.path.join(local_folder_path, file)
        upload_to_snowflake_stage(file_path, snowflake_stage)
        # Optional: Move or delete the file after uploading if needed

    # Add a delay before checking for new files again
    # Adjust the delay based on your requirements
    import time
    time.sleep(60)  # Sleep for 60 seconds before checking again
