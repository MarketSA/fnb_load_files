import os, sys, requests
from datetime import datetime
from data import log, get_campaigns, process_xlsx, process_csv, check_duplicate_data_nopop
from fnb import fnb_process_data



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


def process_from_folder():

    code = 500
    message = ""
    status=  'fail'
    try:
        print('Start running')

        camp_find = get_campaigns()
        if camp_find:
            for i in camp_find['files']:
                message += f"{i['name']}\n"
                file_path = f"{i['folder']}\\{i['fileName'].replace('<YYYYMMDD>', datetime.now().strftime('%Y%m%d'))}"
                if os.path.exists(file_path):
                    print("list_files", file_path)
                    file_data = []
                    
                    if file_path.lower().endswith('xlsx') or file_path.lower().endswith('xls'):
                        file_data = process_xlsx(f'{file_path}')
                    
                    elif file_path.lower().endswith('csv'):
                        file_data = process_csv(f'{file_path}',)
                    
                    else:
                        res = {
                            "message": f"File extension not recognized: {file_path}",
                            "status": 'fail'
                        }
                        code = 404
                        log('[Error in process_from_folder ]', f"File extension not recognized: {file_path}")
                        break

                    for d in file_data:
                        d['inserted_campaign_id'] = f'{i['campaign_id']}'.replace('<MM>', f"{datetime.now().strftime('%B')}").replace('<YYYY>', f"{datetime.now().strftime('%Y')}")   

                        file_data = check_duplicate_data_nopop(file_data, 'contact_id')
                        # check_duplicate_res = check_duplicate_data_nopop(file_data, 'contact_id')
                        # file_data = check_duplicate_res['og_data']
                        # res['dupes'] += check_duplicate_res['dupe_data']
                        # res['dupes_infile'] += len(check_duplicate_res['dupe_data'])
                    message += f"Total records to process: {len(file_data)}\n"
                    return

                    data_res, data_code = fnb_process_data(file_data)
                    message += f"{data_res}\n \n"
                    if data_code == 200:
                        try:
                            # os.remove(f'{folder}/{i}')
                            pass
                        except Exception as e:
                            log('[Error in delete file from process_from_folder ]', f"{e} on line> {e.__traceback__.tb_lineno}")
                    else:
                        message += f'File {i['folder']}\\{i['fileName']} processed with errors'
                else:
                   message +- f'{file_path} does not exist'

                message = "process completed"
                status = "success"

        else:
            message = 'No information found on the campaign'
            status = 'fail'
            code = 404
    except Exception as e:
        message = '[Error in process_from_folder ]' +  f"{e} on line> {e.__traceback__.tb_lineno}"
        status = 'fail'

    send_ntfy("FNB Load Files Process", message, tags=status)
    return res, code


process_from_folder()