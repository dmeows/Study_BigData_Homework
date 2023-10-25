#%% 
import os

def read_log_data():
    folder_path = '/Users/habaokhanh/Study_BigData_Dataset/log_content/'
    log_data = {}

    for day in range(1, 31):
        date = f'202204{day:02d}' 
        filename = f'{date}.json'
        file_path = os.path.join(folder_path, filename)
        
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                log_data[date] = file.read()

    save_path = folder_path + '/combined_log_data.json'
    with open(save_path, 'w') as file:
        file.write(str(log_data))

    return log_data

# Run the function
log_data = read_log_data()

