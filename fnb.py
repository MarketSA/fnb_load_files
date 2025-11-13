from data import  *
from json_file import to_xlsx


def fnb_process_data(data):
    res = {
        "message":"",
        "status":"fail",
        "data": []
    }
    code = 400
    try:
        og_data = len(data)
        res_dm = ""
        DM = None
        insert_res = None
        camp_find = get_campaigns(data)
        if camp_find: 
            if data:
                insert_res = insert_data(camp_find, data)
                DM = insert_Dialler_manager(data[0]['inserted_campaign_id'])
                res_dm = insert_no_data(camp_find, DM)
            else:
                insert_res = "Duplicates where found, but no data was available for insert"
        
            res['data'] = {
                "initial_records": og_data, 
                "insert_rowcount": insert_res,
                "DM": res_dm 
            }
            log("SUCCESS in fnb_credit_card-133_process_main_data  split", f"{res['message'], res['data']['insert_rowcount']}")
            res['status'] = "success"
            code = 200
        else:
            res['data'] = []
            res['status'] = "fail"
            res['message'] = "Error occurred while trying to match campaigns"
            code = 400
    except Exception as e:
        code = 500
        log("Error in fnb_credit_card-133_process_main_data Processing No split", f"{e} on line => {e.__traceback__.tb_lineno}")
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


