
# import param
# from loggers import logger
 
 
# class SnowflakeLoad:                #used for interacting with the database
#     def __init__(self, sc):
#         self.sc = sc
#         self.cursor = self.sc.cursor()
 
#     def create_intials(self):  
#             # put command
#             logger.info("put files to stage")
#             self.cursor.execute("""
#             PUT 'file://C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/GAM.json'@my_json_stage
#                                     """)
           
#             # Check if its loaded to stage
#         #     self.cursor.execute("ls @my_json_stage")
#         #     res = self.cursor.fetchall()
#         #     logger.info(res)
   
#     def copy_to_land(self,sc):  
#             # copy data into gam_landing_data
#             self.cursor.execute("""COPY INTO GAM_landing_data
#                             FROM @my_json_stage/GAM.json.gz
#                             ON_ERROR = 'continue'; """)
           
#     def load_to_final(self):
#             # insert into the gam_final_data from gam_landing_data
#             self.cursor.execute("""
#                              INSERT INTO GAM_FINAL_DATA(payload,
#                             trafficker_name,
#                             trafficker_id,
#                             date_id,
#                             hour_id
#                             )
#                             SELECT
#                             src as payload,
#                             src:trafficker_name::string AS trafficker_name,
#                             src:trafficker_id::string AS trafficker_id,
#                             src:date_id::number AS date_id,
#                             src:hour_id::number AS hour_id
#                             FROM GAM_LANDING_DATA
#                                                             """)

# import param
# from loggers import logger


# class SnowflakeLoad:                #used for interacting with the database
#     def __init__(self, sc):
#         self.sc = sc
#         self.cursor = self.sc.cursor() 

#     def create_intials(self):  
#             # put command
#             logger.info("put files to stage")
#             self.cursor.execute("""
#             PUT 'file://C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/GAM.json'@my_json_stage
#                                     """)
            
#             # Check if its loaded to stage
#             self.cursor.execute("ls @my_json_stage")
#             res = self.cursor.fetchall()
#             logger.info(res)
    
#     def copy_to_land(self,sc):   
#             # copy data into gam_landing_data
#             self.cursor.execute("""COPY INTO GAM_landing_data

#                             FROM @my_json_stage/GAM.json.gz
#                             FILE_FORMAT = (FORMAT_NAME = my_json_format)

#                             ON_ERROR = 'skip_file'; """)
#             logger.info("Copied to raw table created successfully")
           
#     def load_to_final(self):
#             # insert into the gam_final_data from gam_landing_data
#             self.cursor.execute("""
#                              INSERT INTO GAM_FINAL_DATA(payload,
#                             trafficker_name, 
#                             trafficker_id,
#                             date_id,
#                             hour_id
#                             )
#                             SELECT 
#                             src as payload,
#                             src:trafficker_name::string AS trafficker_name, 
#                             src:trafficker_id::string AS trafficker_id,
#                             src:date_id::number AS date_id,
#                             src:hour_id::number AS hour_id
#                             FROM GAM_LANDING_DATA
#                                                             """)


# from loggers import logger
# import param
 
# class SnowflakeLoad:
#     def __init__(self, sc):
#         self.sc = sc
#         self.cursor = self.sc.cursor()
 
#     def create_intials(self):
#         logger.info("put files to stage")
#         self.cursor.execute(""" PUT 'file://C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/GAM.json'@my_json_stage """)
 
#     def copy_to_land(self, sc):
#         self.cursor.execute(f"""COPY INTO {param.table1} FROM @my_json_stage/GAM.json.gz ON_ERROR = 'continue';""")
 
#     def load_to_final(self):
#         self.cursor.execute(f"""
#             INSERT INTO {param.table2} (payload, trafficker_name, trafficker_id, date_id, hour_id)
#             SELECT
#                 src as payload,
#                 src:trafficker_name::string AS trafficker_name,
#                 src:trafficker_id::string AS trafficker_id,
#                 src:date_id::number AS date_id,
#                 src:hour_id::number AS hour_id
#             FROM {param.table1}
#         """)
         
    
# from datetime import datetime
# from loggers import logger
# import param
 
# class SnowflakeLoad:
#     def __init__(self, sc):
#         self.sc = sc
#         self.cursor = self.sc.cursor()
 
#     def create_intials(self):
#         logger.info("put files to stage")
#         self.cursor.execute(""" PUT 'file://C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/GAM.json'@my_json_stage """)
 
#     def copy_to_land(self, sc):
#         # Generate timestamp
#         timestamp = datetime.now().strftime("%Y%m%H%M%S")
        
#         # Modify file name with timestamp
#         file_with_timestamp = f"GAM_{timestamp}.json.gz"
        
#         # Execute the copy command
#         self.cursor.execute(f"""COPY INTO {param.table1} FROM @my_json_stage/{file_with_timestamp} ON_ERROR = 'continue';""")
 
#     def load_to_final(self):
#         self.cursor.execute(f"""
#             INSERT INTO {param.table2}(payload, trafficker_name, trafficker_id, date_id, hour_id)
#             SELECT
#                 src as payload,
#                 src:trafficker_name::string AS trafficker_name,
#                 src:trafficker_id::string AS trafficker_id,
#                 src:date_id::number AS date_id,
#                 src:hour_id::number AS hour_id
#             FROM {param.table1}
#         """)



from datetime import datetime
from gam_activity.loggers import logger
import param
import os
 
class SnowflakeLoad:
    def __init__(self,sc):
        self.sc = sc
        self.cursor = self.sc.cursor()
 
    def create_intials(self,file_path):
        logger.info("put files to stage")
        self.cursor.execute(f""" PUT 'file://{file_path}' @my_json_stage """)
 
   
   
    # def copy_to_land(self,sc):
    #     timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    #     file_name = f"GAM_{timestamp}.json.gz"
    #     file_path = f"@my_json_stage/{file_name}"
    #     self.cursor.execute(f"""COPY INTO {param.table1} FROM '{file_path}' ON_ERROR = 'continue';""")
        
    def copy_to_land(self,sc,file_path):   
            # copy data into gam_landing_data
            self.cursor.execute(f"""COPY INTO {param.table1}

                            FROM @my_json_stage/{file_path}
                            

                            ON_ERROR = 'continue'; """)
            logger.info("Copied to raw table created successfully")

    def load_to_final(self):
        self.cursor.execute(f"""
            INSERT INTO {param.table2}(payload, trafficker_name, trafficker_id, date_id, hour_id)
            SELECT
                src as payload,
                src:trafficker_name::string AS trafficker_name,
                src:trafficker_id::string AS trafficker_id,
                src:date_id::number AS date_id,
                src:hour_id::number AS hour_id
            FROM {param.table1}
        """)