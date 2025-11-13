from data import  *
from json_file import to_xlsx


def findDupes(db_data, campID):
    res = {
        "dupes": [],
        "data": [],
        "count": 0,
        "dupes_infile": 0,
        "dupes_inDB": 0,
        "status": "fail",
    }
    try:
        final_og_leads =  []
        print(f'\nStarting fnb_bcpp Dedupe {datetime.now()}')

        for i in db_data:
            i['inserted_campaign_id'] = f'{campID}'

            check_duplicate_res = check_duplicate_data_nopop(db_data, 'contact_id')
            db_data = check_duplicate_res['og_data']
            res['dupes'] += check_duplicate_res['dupe_data']
            res['dupes_infile'] += len(check_duplicate_res['dupe_data'])
            
            final_og_leads += db_data


        print(f'Finished fnb_bcpp Dedupe {datetime.now()} for {len(db_data)} leads')
        final_dupes = check_duplicate_data_nopop(res['dupes'], 'contact_id')
        db_dupes = len(final_dupes['og_data']) - res['dupes_infile'] 
        res["dupes_inDB"] = db_dupes
        res["data"] = final_og_leads
        res['dupes'] = final_dupes['og_data']
        res["count"] = len(res['dupes'])
        res["status"] = 'success'


    except Exception as e:
        log(" fnb_bcpp Error in finding Duplicates", f"{e} on line => {e.__traceback__.tb_lineno}")
        res = {
            "dupes": [],
            "data": [],
            "count": -1,
            "status": 'ERROR'
        }

    return res


def fnb_process_data(data):
    res = {
        "message":"",
        "status":"fail",
        "data": []
    }
    code = 400
    try:
        og_data = len(data['data'])
        res_dm = ""
        DM = None
        insert_res = None
        camp_find = get_campaigns(data)
        if camp_find:
            
            
            dupes = findDupes(data['data'], data['x_campaign_id'])
            
            if dupes['dupes']:
                to_xlsx(dupes['dupes'], 'fnb_bcpp_processed_data')
            
            if dupes['status'] == 'ERROR':
                res['message'] = "Error while Depuping Data, please make sure the file is in the correct Formart"
                res['data'] = []
                res['status'] = "fail"
                code = 404
            
            elif dupes['status'] == 'success':    
                res['message'] = f" {dupes['count']} duplicates found in the dataset  <br> {dupes['dupes_infile']} found in File <br> And {dupes['dupes_inDB']} found in Database"
                percentages = ""
                if dupes['data']:
                    insert_res = insert_data(camp_find, dupes['data'])
                    DM = insert_Dialler_manager(dupes['data'][0]['inserted_campaign_id'])
                    res_dm = insert_no_data(camp_find, DM)
                else:
                    insert_res = "Duplicates where found, but no data was available for insert"
            
                res['data'] = {
                    "initial_records": og_data, 
                    "dupe_rowcount": dupes['count'],
                    "insert_rowcount": insert_res,
                    "DM": res_dm 
                }
                log("SUCCESS in fnb_bcppI_process_main_data  split", f"{res['message'], res['data']['insert_rowcount']} percentages: {percentages}")
                res['status'] = "success"
                code = 200
        else:
            res['data'] = []
            res['status'] = "fail"
            res['message'] = "Error occurred while trying to match campaigns"
            code = 400
    except Exception as e:
        code = 500
        log("Error in fnb_bcppI_process_main_data Processing No split", f"{e} on line => {e.__traceback__.tb_lineno}")
        res['data'] = []
        res['status'] = "error"
        res['message'] = "Error occurred while trying to match campaigns"
        
    return res, code


def insert_Dialler_manager(CampeignID):
    sql = f"""
        INSERT INTO DiallerManager ( CustomerID, DialNumber, NoType, CallResult, CallCount, TsrId, FormName, CampeignID )
        SELECT Contacts.ContactID, Contacts.TelCell, 'C'+[CampeignID], '0', '0', '0', '8', '10'
        FROM Contacts
        WHERE Contacts.TelCell Like '0%' 
        AND Contacts.LeadResultID Is Null 
        AND Contacts.CampeignID = '{CampeignID}'
        AND CAST(Import_Date as Date) = CAST(GETDATE() as Date);


        INSERT INTO DiallerManager ( CustomerID, DialNumber, NoType, CallResult, CallCount, TsrId, FormName, CampeignID )
        SELECT Contacts.ContactID, Contacts.TelHome, 'H'+[CampeignID], '0', '0', '0', '8', '10'
        FROM Contacts
        WHERE Contacts.TelHome Like '0%' 
        AND Contacts.LeadResultID Is Null 
        AND Contacts.CampeignID = '{CampeignID}'
        AND CAST(Import_Date as Date) = CAST(GETDATE() as Date);


        INSERT INTO DiallerManager ( CustomerID, DialNumber, NoType, CallResult, CallCount, TsrId, FormName, CampeignID )
        SELECT Contacts.ContactID, Contacts.TelWork, 'W'+[CampeignID], '0', '0', '0', '8', '10'
        FROM Contacts
        WHERE Contacts.TelWork Like '0%'
        AND Contacts.LeadResultID Is Null 
        AND Contacts.CampeignID = '{CampeignID}'
        AND CAST(Import_Date as Date) = CAST(GETDATE() as Date);

    """
    return sql


