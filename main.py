from connectivity import snowflake_conn
from utils import create_folder_structure
#from gam_activity.loggers import modified
from gam_activity.loggers import logger
from extract import extract_file
from gam_activity.transformation import Tranformation
from gam_activity.landtofinal import SnowflakeLoad
from gam_activity.validation import Validation
import datetime
import os
from utils import slack_notify
from utils import slack_notify_alert
from utils import slack_notify_empty_data
from taskscheduler import task
from datetime import datetime
from utils import modified
#from gam_activity.loggers import modified
from utils import create_folder_structure


def main(): 
    
    if snowflake_conn:
        logger.info("Connected to Snowflake successfully.")
        try:
            cur = snowflake_conn.cursor()
            logger.info("Code is Running")
            # extraction of the file
            df = extract_file()
            # Check if DataFrame is not empty  
            if not df.empty:  
                date_column = 'Date'
                date_id_column = 'date_id'
                hour_id_column = 'hour_id'
                Trafficker = 'Trafficker'
                trafficker_name = 'trafficker_name'
                trafficker_id = 'trafficker_id'
                
                transformation_processor = Tranformation(df)
    
                # Perform data transformations
                transformation_processor.replace_nan_with_null()
                df['Date'] = df['Date'].apply(transformation_processor.conversion)
                df = transformation_processor.process_date_column(date_column, date_id_column)
                df = transformation_processor.add_hour_id_column(date_column, hour_id_column)
                df = transformation_processor.process_trafficker_column(Trafficker, trafficker_name, trafficker_id)
                df = transformation_processor.to_snake_case()
                logger.info(df)
                # Convert DataFrame to JSON and save to file

                # timestamp=get_time()
                # #timestamp=datetime.now().strftime('%Y%m%d%H%M%S')
                # #file_path=df.to_json(f'C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/GAM_{timestamp}.json', orient='records', lines=True)
                # file_path = f'C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/gam_files/GAM_{timestamp}.json'
                file_path=modified()
                df.to_json(file_path, orient='records', lines=True)
                # base_dir=f"C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/gam_log_files"
                # create_folder_structure(base_dir)
            
                # base_dir=os.path.dirname(file_path)
                # base_dir=base_dir.replace("gam_files","gam_log_files")
                # create_folder_structure(base_dir)
                # log_file_path = modified()
                # print("New log file created:", log_file_path)



                # perform landtofinal
                snowflake_loader = SnowflakeLoad(snowflake_conn)
                snowflake_loader.create_intials(file_path) #create_intials
                logger.info("create_intials done")
                snowflake_loader.copy_to_land(df,file_path)
                logger.info(".copy_to_land done")
                snowflake_loader.load_to_final()
                logger.info("load_to_final created")
                # perform the validation            
                ValidUse=Validation(cur)
                ValidUse.validate_null_values()
                logger.info("null data validated")
                ValidUse.validate_date_id()
                logger.info("validate_date_id")
                ValidUse.validate_trafficker_id()
                logger.info("valiadated_trafficker_id")
                logger.info("Snowflake loading completed successfully.")
            # Give warning if the dataframe is empty
            else:
                logger.warning("There is no data to processed")
                slack_notify_empty_data()
            # notify in the slack about the successfull loading of the data
            slack_notify()
            cur.close()
            snowflake_conn.close()
            
        # Condition failed notify alert generate and a logger of connection failed
        except Exception as e:
            logger.error("Connection to Snowflake failed.")
            slack_notify_alert(e)
            logger.info(f"Error: {e}")
       
if __name__ == "__main__":
    main()
    task()
   