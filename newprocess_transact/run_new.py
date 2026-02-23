import os, requests, sys
from datetime import datetime, timedelta
from data import get_campaigns, process_xlsx, process_csv, check_duplicate_data_nopop
from fnb_new import fnb_process_data
from sendemail import sendEMail
from pathlib import Path



def send_ntfy(title, message, tags='inbox_tray'):
    try:
        requests.post('http://34.58.235.239/reports',
        data=message,
        headers={
            "Title": title,
            "Tags":tags
            # "Priority": "urgent",s
        })
    except: pass

def is_similar(file1: str = "", file2: str = "") -> bool:
    name_without_ext = Path(file2).stem
    return file1.startswith(name_without_ext)


def process_from_folder(sub):

    code = 500
    message = ""
    status=  'fail'
    try:
        print('Start running')

        camp_find = get_campaigns()
        if camp_find:
            for i in camp_find['files']:
                message += f"{i['name']}\n"
                folder = f"{i['folder']}\\{i['subfolder']}"
                # for fol in os.listdir(folder):
                print('processing folder', folder) 
                # file_path = f"{folder}\\{fol}" 

                timespec = (datetime.now() - timedelta(int(sub)))
                print('timespec',timespec)
                toreplacetime = f"{timespec.strftime(i['date_format'])}".upper()
                replacedFileName = i['fileName'].replace('<YYYYMMDD>', toreplacetime)   
                file_path = f"{i['folder']}\\{i['subfolder']}\\{replacedFileName}" 
                
                if not os.path.exists(file_path):
                    files = os.listdir(folder)
                        # print(files)
                    similar_files = [file for file in files if is_similar(file, replacedFileName)]
                    if similar_files:
                        file_path = f"{i['folder']}\\{i['subfolder']}\\{similar_files[0]}"
                        replacedFileName = f"{similar_files[0]}"
                    # print('similar files', similar_files)

                if i['active']:
                    if os.path.exists(file_path):
                        print("list_files", file_path)
                        file_data = []
                        
                        if file_path.lower().endswith('xlsx') or file_path.lower().endswith('xls'):
                            file_data = process_xlsx(f'{file_path}')
                        
                        elif file_path.lower().endswith('csv'):
                            file_data = process_csv(f'{file_path}',)
                        
                        else:
                            message +=  f"File extension not recognized: {file_path}",
                            status +=  'fail'
                            break
                        
                        for d in file_data:
                            d['inserted_campaign_id'] = f'{i['campaign_id']}'.replace('<MM>', f"{timespec.strftime('%B')}").replace('<YYYY>', f"{timespec.strftime('%Y')}")   
                            d['filename'] = f"{replacedFileName}"
                            
                        file_data = check_duplicate_data_nopop(file_data, i['idcolumn'])

                        message += f"Filename: {file_path} Total records to process: {len(file_data)}\n"

                        print('process data here - - - - - ')
                        # return
                        data_res, data_code = fnb_process_data(file_data, i)
                        print(data_res)
                        # data_res = None
                        message += f"{data_res}\n \n \n \n"
                    else:
                        message += f'{file_path} does not exist \n \n'
                        message += "process completed"
                        status = "success"
                else:
                    message += f'{file_path} Process is deactivated \n \n'
                    message += "process completed"
                    status = "success"
            

        else:
            message = 'No information found on the campaign'
            status = 'fail'
    except Exception as e:
        message = '[Error in process_from_folder ]' +  f"{e} on line> {e.__traceback__.tb_lineno}"
        status = 'fail'
        print(message)
        
    sendEMail(['givenk@marketsa.co.za', 'austinp@marketsa.co.za'], message.replace('\n', '<br>'), 'FNB Leads New transact Process Result')
    send_ntfy("FNB Load Files Process", message, tags=status)
    print('Processing completed')


process_from_folder(sys.argv[1])

print(sys.argv[1], flush=True)