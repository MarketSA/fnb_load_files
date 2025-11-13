import os, sys
from data import log, get_campaigns, process_xlsx, process_txt, process_csv
from fnb_bcpp import fnb_bcpp_process_data


def process_from_folder():
    code = 500
    res = {
        "message": "",
        "status": 'fail',
        "files": []
    }
    try:
        data = {"id": 1}
        print('Start running', data)

        camp_find = get_campaigns('single', data)
        if camp_find:
            folder = f"./processfiles/{camp_find['main_folder']}/{camp_find['folder']}"
            list_files = os.listdir(f'{folder}')
            if list_files:
                for i in list_files:
                    file_data = []
                    
                    if i.lower().endswith('xlsx') or i.lower().endswith('xls'):
                        file_data = process_xlsx(f'{folder}/{i}')
                    
                    elif i.lower().endswith('csv'):
                        file_data = process_csv(f'{folder}/{i}',)
                    
                    else:
                        res = {
                            "message": f"File extension not recognized: {folder}/{i}",
                            "status": 'fail'
                        }
                        code = 404
                        log('[Error in process_from_folder ]', f"File extension not recognized: {folder}/{i}")
                        break

                    data['data'] = file_data
                    data_res, data_code = fnb_bcpp_process_data(data)
                    
                    if data_code == 200:
                        try:
                            # os.remove(f'{folder}/{i}')
                            pass
                        except Exception as e:
                            log('[Error in delete file from process_from_folder ]', f"{e} on line> {e.__traceback__.tb_lineno}")
                    else:
                        data_res['error'] = f'File {i} processed with errors'
                    data_res['file_name'] = f"{i}"
                    data_res['camp_name'] = f"{camp_find['name']}"
                    res['files'].append(data_res)
                
                res['message'] = "process completed"
                res['status'] = "success"
                code = 200
            else:
                res = {
                    "message": f"No files found for campaign in the folder: {folder}",
                    "status": 'fail'
                }
                code = 404
        else:
            res['message'] = 'No information found on the campaign'
            res['status'] = 'fail'
            code = 404
    except Exception as e:
        log('[Error in process_from_folder ]', f"{e} on line> {e.__traceback__.tb_lineno}")
        res = {
            "message": f"{e}",
            "status": 'error*'
        }
        code = 500
    return res, code