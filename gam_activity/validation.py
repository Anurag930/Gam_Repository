# import logging
# from loggers import logger
# import param
 
# class Validation:

#     def __init__(self,cur):
#          self.cursor=cur
#         # Initialize your class attributes, like self.cursor
 
#     def execute_snowflake_query(self, query):
#             self.cursor.execute(query)
#             return self.cursor.fetchall()
 
#     def validate_null_values(self):
#         logger.info("Validating null values\n")
#         # Execute SQL query to count null values in payload
#         count_query = """
#            SELECT COUNT(*) AS count
#             FROM {param.table2} a, lateral flatten(input => payload) b
#             WHERE b.value::string IN ('NULL', 'NA', 'N/A', 'NaN', 'nan', '');
#         """
#         count_res = self.execute_snowflake_query(count_query)
 
#         if count_res[0][0]==0:
#             logger.info("zero null values found Validation successful")
#         else:
#             logger.info(count_res+" found Validation not successful")
 
#     def validate_date_id(self):
#         logger.info("Validating date_id values\n")
 
#         # Execute SQL query for DATE_ID validation
#         date_id_query = """
#             SELECT 'DATE_ID' AS FIELD, COUNT(*) AS NO_OF_MISSMATCH
#             FROM {param.table2}
#             WHERE TRY_TO_DATE(DATE_ID::STRING, 'YYYYMIDD') IS NULL AND DATE_ID::STRING IS NOT NULL;
#         """
#         date_id_validation_result = self.execute_snowflake_query(date_id_query)
 
#         # Check the validation result for DATE_ID
#         if date_id_validation_result[0][1] == 0:
#             logger.info("DATE_ID Validation successful")
#         else:
#             logger.info("DATE_ID Validation not successful")
 
#     def validate_trafficker_id(self):
#         logger.info("Validating trafficker_id values\n")
 
#         # Execute SQL query for TRAFFICKER_ID validation
#         trafficker_id_query = """
#            SELECT 'TRAFFICKER' AS FIELD, COUNT(*) AS NO_OF_MISSMATCH
#            FROM {param.table2}
#            WHERE PAYLOAD:TRAFFICKER::STRING NOT LIKE'%@%.com%';
#         """
#         trafficker_id_validation_result = self.execute_snowflake_query(trafficker_id_query)
 
#         # Check the validation result for TRAFFICKER_ID
#         if trafficker_id_validation_result[0][1] ==0:
#             logger.info("TRAFFICKER_ID Validation successful")
#         else:
#             logger.info("TRAFFICKER_ID Validation not successful")

import logging
from gam_activity.loggers import logger
import param
 
class Validation:
    def __init__(self, cur):
        self.cursor = cur
 
    def execute_snowflake_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
 
    def validate_null_values(self):
        logger.info("Validating null values\n")
        count_query = """
            SELECT COUNT(*) AS count
            FROM {table2} a, lateral flatten(input => payload) b
            WHERE b.value::string IN ('NULL', 'NA', 'N/A', 'NaN', 'nan', '');
        """.format(table2=param.table2)
        count_res = self.execute_snowflake_query(count_query)
        if count_res[0][0] == 0:
            logger.info("Zero null values found. Validation successful")
        else:
            logger.info(f"{count_res[0][0]} found. Validation not successful")
 
    def validate_date_id(self):
        logger.info("Validating date_id values\n")
        date_id_query = """
            SELECT 'DATE_ID' AS FIELD, COUNT(*) AS NO_OF_MISMATCH
            FROM {table2}
            WHERE TRY_TO_DATE(DATE_ID::STRING, 'YYYYMIDD') IS NULL AND DATE_ID::STRING IS NOT NULL;
        """.format(table2=param.table2)
        date_id_validation_result = self.execute_snowflake_query(date_id_query)
        if date_id_validation_result[0][1] == 0:
            logger.info("DATE_ID Validation successful")
        else:
            logger.info("DATE_ID Validation not successful")
 
    def validate_trafficker_id(self):
        logger.info("Validating trafficker_id values\n")
        trafficker_id_query = """
            SELECT 'TRAFFICKER' AS FIELD, COUNT(*) AS NO_OF_MISMATCH
            FROM {table2}
            WHERE PAYLOAD:TRAFFICKER::STRING NOT LIKE '%@%.com%';
        """.format(table2=param.table2)
        trafficker_id_validation_result = self.execute_snowflake_query(trafficker_id_query)
        if trafficker_id_validation_result[0][1] == 0:
            logger.info("TRAFFICKER_ID Validation successful")
        else:
            logger.info("TRAFFICKER_ID Validation not successful")