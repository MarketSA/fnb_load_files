from datetime import datetime
import csv, os, json 
import pandas as pd
# tablib
 


def to_csv(data, filename):
    name = get_filename(filename, 'csv')
    data_file = open(f'{name}', 'w' , encoding='UTF8', newline='')
    
    csv_writer = csv.writer(data_file)
    count = 0
    for i in data:
        # print(i)
        if count == 0:
            # Writing headers of CSV file
            header = i.keys()
            csv_writer.writerow(header)
            count += 1
        csv_writer.writerow(i.values())
    
    data_file.close()


def to_xlsx(data, filename):
    try:
        name = get_filename(filename, 'xlsx')
        pd.DataFrame(data).to_excel(f"{name}",index=False)
    except Exception as e:
        name = get_filename(filename, 'json')
        with open(f"{name}", 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        # print(f'\n\nERROR in XLSX {e} on line {e.__traceback__.tb_lineno}')

 

def get_filename(file, ext):
    path = f"./dupe_data/{file}/"
    try:
        os.makedirs(path)
    except:
        pass
    filename = f'{file}_{datetime.now().strftime("%Y%B%d")}'
    if os.path.exists(f"{path}{filename}.{ext}"):
        for num, i in enumerate(range(10000), start=1):
            tempname = f"{filename}_({num})"
            if os.path.exists(f"{path}{tempname}.{ext}"):
                continue
            else:
                filename = tempname
                break
    return f"{path}{filename}.{ext}"