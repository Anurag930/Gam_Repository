import param
from gam_activity.loggers import logger


class SnowflakeLoad:                #used for interacting with the database
    def __init__(self, sc):
        self.sc = sc
        self.cursor = self.sc.cursor() 

    def create_intials(self):  
            # put command
            logger.info("put files to stage")
            self.cursor.execute("""
            PUT 'file://C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/GAM.json'@my_json_stage
                                    """)
            
            # Check if its loaded to stage
            self.cursor.execute("ls @my_json_stage")
            res = self.cursor.fetchall()
            logger.info(res)
    
    def copy_to_land(self,sc):   
            # copy data into gam_landing_data
            self.cursor.execute("""COPY INTO GAM_landing_data

                            FROM @my_json_stage/GAM.json.gz
                            FILE_FORMAT = (FORMAT_NAME = my_json_format)

                            ON_ERROR = 'skip_file'; """)
            logger.info("Copied to raw table created successfully")
           
    def load_to_final(self):
            # insert into the gam_final_data from gam_landing_data
            self.cursor.execute("""
                             INSERT INTO GAM_FINAL_DATA(payload,
                            trafficker_name, 
                            trafficker_id,
                            date_id,
                            hour_id
                            )
                            SELECT 
                            src as payload,
                            src:trafficker_name::string AS trafficker_name, 
                            src:trafficker_id::string AS trafficker_id,
                            src:date_id::number AS date_id,
                            src:hour_id::number AS hour_id
                            FROM GAM_LANDING_DATA
                                                            """)
         
    
