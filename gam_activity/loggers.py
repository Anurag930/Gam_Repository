# import logging  #For importing log Message

# logger = logging

# # logging basic config method and saving log files
# logger.basicConfig(filename=r'C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/gam_files/pipeline.log', level=logging.INFO,
#                     format=f'%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
# logger.basicConfig(filename=r'C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/gam_files/pipeline.log', level=logging.ERROR,
# format=f'%(asctime)s:%(funcName)s:%(levelname)s:%(lineno)d')



# import logging  #For importing log Message
# from datetime import datetime
# from utils import modified
# import os



# logger = logging
# file_path=modified()
# log_dir="gam_log_files/{}/{}/{}/{}/{}.format(datetime.now().year,datetime.now().month,datetime.now().day,datetime.now().hour,datetime.now().minute)"
# os.makedirs(log_dir,exist_ok=True)
# log_file_path=os.path.join(log_dir,os.path.basename(file_path).replace('.json','.log'))

# # logging basic config method and saving log files
# logger.basicConfig(filename=log_file_path, level=logging.INFO,
#                     format=f'%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
# logger.basicConfig(filename=log_file_path, level=logging.ERROR,
# format=f'%(asctime)s:%(funcName)s:%(levelname)s:%(lineno)d')



import logging
from datetime import datetime
from utils import modified
import os
 
logger = logging.getLogger()  # Get the root logger
 
file_path = modified()
log_dir = "gam_log_files/{}/{}/{}/{}/{}".format(datetime.now().year, datetime.now().month, datetime.now().day, datetime.now().hour, datetime.now().minute)
os.makedirs(log_dir, exist_ok=True)
 
log_file_path = os.path.join(log_dir, os.path.basename(file_path).replace('.json', '.log'))
 
# Combine configurations into one call
logging.basicConfig(filename=log_file_path, level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
 
# Alternatively, you can set the level and format separately
# logger.setLevel(logging.ERROR)
# formatter = logging.Formatter('%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
# file_handler = logging.FileHandler(log_file_path)
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)






# import logging
# import os
# from datetime import datetime
# from utils import create_folder_structure
 
# # Set up logging
# logger = logging
# file_path = create_folder_structure()
# log_file_path = file_path.replace('.json', '.log')
# logger.basicConfig(filename=log_file_path, level=logging.INFO,
#                     format=f'%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
# logger.basicConfig(filename=log_file_path, level=logging.ERROR,
# format=f'%(asctime)s:%(funcName)s:%(levelname)s:%(lineno)d')
 
# # Function to create folder structure
# def create_folder_structure(base_dir):
#     now = datetime.now()
#     year = str(now.year)
#     month = str(now.month).zfill(2)  # Zero-padding month
#     day = str(now.day).zfill(2)      # Zero-padding day
#     hour = str(now.hour).zfill(2)    # Zero-padding hour
#     minute =str(now.minute).zfill(2)
#     # Construct folder path
#     folder_path = os.path.join(base_dir, year, month, day, hour,minute)
 
#     # Create folder if it doesn't exist
#     os.makedirs(folder_path, exist_ok=True)
 
#     return folder_path
 



# import logging
# import os
# from datetime import datetime
# logger=logging
 
# def modified():
#     timestamp = datetime.now().strftime('%Y%m%d%H%M')
#     file_path = f'C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/gam_log_files/GAM_{timestamp}.json'
#     return file_path
 
# def create_folder_structure(base_dir):
#     now = datetime.now()
#     year = str(now.year)
#     month = str(now.month).zfill(2)  # Zero-padding month
#     day = str(now.day).zfill(2)      # Zero-padding day
#     hour = str(now.hour).zfill(2)    # Zero-padding hour
#     minute = str(now.minute).zfill(2)    # Zero-padding minute
 
#     # Construct folder path
#     folder_path = os.path.join(base_dir, year, month, day, hour, minute)
 
#     # Create folder if it doesn't exist
#     os.makedirs(folder_path, exist_ok=True)
 
#     # Create log file path inside the minute folder
#     log_file_path = os.path.join(folder_path, f'GAM_{year}{month}{day}{hour}{minute}.log')
 
#     # Configure logging
#     logging.basicConfig(filename=log_file_path, level=logging.INFO,
#                         format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
 
#     # Additional configuration for logging errors
#     logging.getLogger().addHandler(logging.StreamHandler())  # Print errors to console
#     logging.basicConfig(filename=log_file_path, level=logging.ERROR,
#                         format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s:%(lineno)d')
 
#     return folder_path
 

# import os
# from datetime import datetime
# import logging

# logger=logging

 
# def create_folder_structure():
#     now = datetime.now()
#     year = str(now.year)
#     month = str(now.month).zfill(2)  # Zero-padding month
#     day = str(now.day).zfill(2)      # Zero-padding day
#     hour = str(now.hour).zfill(2)    # Zero-padding hour
#     minute = str(now.minute).zfill(2)    # Zero-padding minute
 
#     # Construct folder path
#     folder_path = os.path.join("C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/gam_log_files", year, month, day, hour, minute)
 
#     # Create folder if it doesn't exist
#     os.makedirs(folder_path, exist_ok=True)
 
#     # Construct file path inside the minute folder
#     file_path = os.path.join(folder_path, f'GAM_{year}{month}{day}{hour}{minute}.log')
 
#     return file_path
 




# import os
# from datetime import datetime
# import logging
 
# logger = logging
 
# def modified():
#     now = datetime.now()
#     year = str(now.year)
#     month = str(now.month).zfill(2)  # Zero-padding month
#     day = str(now.day).zfill(2)      # Zero-padding day
#     hour = str(now.hour).zfill(2)    # Zero-padding hour
#     minute = str(now.minute).zfill(2)    # Zero-padding minute
    
#     # Construct folder path
#     folder_path = os.path.join(f'/Users/anurag.sharma/Documents/test_python_scripts/GAM/gam_log_files', year, month, day, hour, minute)
 
#     # Create folder if it doesn't exist
#     os.makedirs(folder_path, exist_ok=True)


#     logger.basicConfig(filename=f'{folder_path}.log', level=logging.INFO,
#                     format=f'%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
#     logger.basicConfig(filename=f'{folder_path}.log', level=logging.ERROR,
#     format=f'%(asctime)s:%(funcName)s:%(levelname)s:%(lineno)d')




#     log_file_path = os.path.join(folder_path, f'GAM_{year}{month}{day}{hour}{minute}.log')
 
#     return log_file_path
