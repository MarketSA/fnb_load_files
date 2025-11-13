
from datetime import datetime
import pyodbc, pandas, json
from flask import current_app as capp

def log(log, err= None):
    tm = datetime.now().strftime("%Y_%m_%d")
    f = open(f"./logs/log_{tm}.log", "a")
    f.write(f"\n{str(datetime.now())} || {log} => ||  {err} ")
    f.close()


def db_Connection(server):
    con_string = f"SERVER={server['server']};DATABASE={server['dbname']};UID={server['username']};PWD={server['password']}"
    cnxn = pyodbc.connect('DRIVER={SQL Server};'+con_string+";")
    return cnxn


def get_campaigns(rem=None, data = None):
    campaigns = {}
    file = []
    with open('campaigns.json') as json_file:
        file = json.load(json_file)
    
    if rem == "single":
        for i in file:
            camp_find = None
            if i["id"] == int(data['id']):
                campaigns = i
                break
            if camp_find: 
                campaigns = camp_find
                campaigns['main_folder'] = f"{i['title']}".lower()
                break
            else: continue
    return campaigns



def change_phone_number(phone, addZero=None):
    res = ""
    if phone:
        phone = f"{phone}".replace(" ", "").strip()
        if f"{phone}".startswith('+27(0)'):
            res = f"0{phone[6:]}"

        elif f"{phone}".startswith('0027'):
            phone = phone[4:]
            res = f"0{phone}"

        elif f"{phone}".startswith('000027'):
            phone = phone[6:]
            res = f"0{phone}"

        elif f"{phone}".startswith('00027'):
            phone = phone[5:]
            res = f"0{phone}"
        
        elif len(phone) > 10 and not (f"{phone}".startswith('0')):
            phone = list(phone)
            phone[0] = ""
            phone[1] = "0"
            res = ''.join(phone)
        
        elif len(phone) > 10 and (f"{phone}".startswith('00')):
            phone = list(phone)
            phone[0] = ""
            phone[1] = "0"
            res = ''.join(phone)
        
        elif f"{phone}".startswith('27'):
            phone = phone[2:]
            res = f"0{phone}"
        
        elif f"{phone}".startswith('+27'):
            phone = phone[3:]
            res = f"0{phone}"
        
        elif not (f"{phone}".startswith('0')) and len(phone) == 9:
            res = f"0{phone}"
        
        else:
            res = phone
    else:
        res = phone
    return res


    
def strip_dict_keys(old_dict):
    new_dict = {}
    for key, value in zip(old_dict.keys(), old_dict.values()):
        new_key = f"{key}".strip()
        new_dict[new_key] = old_dict[key]
    return new_dict

def get_ID_as_values(data, columnNames):
    res = ''
    for i in data:
        res += "("
        for ii in columnNames:
            i[ii] = f"{i[ii]}".strip()
            res += f"'{i[ii]}',"
        res = res[:-1]
        res += "),"
    return res[:-1]


def check_duplicate_data_nopop(data = [], columnName="", pop=False):
    res = {
        "og_data":[],
        "dupe_data":[]
    }
    for i in data:
        count = 0
        toPop = []
        for index, dup in enumerate(data, start=0):
            if i[columnName] == dup[columnName]:
                count +=1
            if count > 1:
                i['reason'] = "Exists in File"
                i['db_campeignid'] = ""
                i['leadresultid'] = f"unusable"
                i['operator'] =  f"Data"
                i['sale_date'] = f"{datetime.now().strftime('%Y-%m-%d')}"

                res["dupe_data"].append(i)
                if not (index in toPop):
                    toPop.append(index)
                count -=1
        toPop.sort()
        for index, t in enumerate(toPop, start=0):
            if pop:
                data.pop((t-index))
    res['og_data'] = data
    return res

def create_insert_string(formart, data):
    res = ""
    for i in data:
        res += "("
        for col in formart:
            if f"{i[col]}".strip() == ".":
                i[col] = None
                
            if i[col] == "" or i[col] == None:
                res += f"NULL,"
            else:
                i[col] = f"{i[col]}".replace("'", "").strip()
                if col == "acquired_companyname":
                    res += f"'{i[col][0:19]}',"
                else:
                    res += f"'{i[col]}',"
        
        res = f"{res[:-1]}"
        res += "),"
    if res.endswith(','):
        res = f"{res[:-1]};"
    return res

def insert_data(server, db_data, insert_formart = 'insert_formart', file_columns='file_columns'):
    res = {}
    try:
        for i in server['table']:
            if i['insert_formart']:
                print('')   
                sql = []
                
                if len(db_data) < 1000:
                    db_data = create_insert_string(i[file_columns], db_data)
                    sql.append(f"""INSERT INTO [{server['dbname']}].dbo.[{i['name']}] {i[insert_formart]} VALUES {db_data}""")
                else:
                    val = 1000
                    if len(db_data) > val:
                        num = round(len(db_data)/val)
                        count = 0
                        for r in range(num+1):
                            h = 0
                            if count > len(db_data):
                                break
                            h = (val + count)
                            insert_res = create_insert_string(i[file_columns], db_data[int(count):int(h)])
                            if insert_res:
                                sql.append(f"""INSERT INTO [{server['dbname']}].dbo.[{i['name']}] {i[insert_formart]} VALUES {insert_res}""")
                            count = h
                
                cnxn = db_Connection(server)
                cursor = cnxn.cursor()
                res_count = 0
                cursor.execute('set ANSI_WARNINGS  OFF')
                cnxn.commit()

                for ins in sql:
                    res_count += cursor.execute(ins).rowcount
                    cnxn.commit()

                res[i['name']] = res_count

                cursor.execute('set ANSI_WARNINGS  ON')
                cnxn.commit()
                cnxn.close()
              
            sql = []
    except Exception as e:
        log(f"Error in Inserting Data {server['name']} camp_ID=> {server['id']} ||", f"{e} on line => {e.__traceback__.tb_lineno}")
        res = 'ERROR'
    return res

def insert_no_data(server, sql):
    res = {}
    try:
        # if True:
        if not capp.debug:
            # print(sql)
            cnxn = db_Connection(server)
            cursor = cnxn.cursor()
            count = cursor.execute(sql).rowcount
            cnxn.commit()
            cnxn.close()
            res = count
        else:
            res = "Server in debug mode"
            print("app in debug mode")
    except Exception as e:
        log(f"Error in Inserting no Data camp_ID=> {server['id']} ||", f"{e} on line => {e.__traceback__.tb_lineno}")
        res = 'ERROR'
    return res

def process_xlsx(file):
    json_str = []
    try:
        excel_data_fragment = pandas.read_excel(f'{file}', sheet_name=0, dtype=str)
        json_str = excel_data_fragment.to_json(orient='records')
        json_str = json.loads(json_str)
        #make all keys lower_case?
        temp_data = {"data": json_str}
        df = pandas.DataFrame(temp_data)
        sf = df["data"].apply(lambda x: {k.lower().replace('\xa0', ''): v for k, v in x.items()})
        res = sf.to_json(orient="records")
        res = json.loads(res)
        return res
    except Exception as e:
        log('[Error in process_xlsx ]', f"{e} on line> {e.__traceback__.tb_lineno} || file=> {file}")
    return json_str


def process_csv(file, deli=','):
    json_str = []
    try:
        excel_data_fragment = pandas.read_csv(f'{file}',  delimiter=f"{deli}", index_col=False, dtype=str, encoding='latin-1')
        json_str = excel_data_fragment.to_json(orient='records')
        json_str = json.loads(json_str)

        temp_data = {"data": json_str}
        df = pandas.DataFrame(temp_data)
        sf = df["data"].apply(lambda x: {k.lower().strip(): v for k, v in x.items()})
        res = sf.to_json(orient="records")
        res = json.loads(res)
        return res
    except Exception as e:
        log('[Error in process_csv ]', f"{e} on line> {e.__traceback__.tb_lineno} || file=> {file}")
    return json_str
