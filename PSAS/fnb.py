from data import  *

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
                print('inserting data')
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
            print("SUCCESS in fnb_credit_card-133_process_main_data ", f"{res['message'], res['data']['insert_rowcount']}")
            res['status'] = "success"
            code = 200
        else:
            res['data'] = []
            res['status'] = "fail"
            res['message'] = "Error occurred while trying to match campaigns"
            code = 400
    except Exception as e:
        code = 500
        print("Error in fnb_credit_card-133_process_main_data Processing", f"{e} on line => {e.__traceback__.tb_lineno}")
        res['data'] = []
        res['status'] = "error"
        res['message'] = "Error occurred while trying to match campaigns"
        
    return res, code


def insert_Dialler_manager(CampeignID):
    sql = f"""
        INSERT INTO DiallerManager ( CustomerID, DialNumber, NoType, CallResult, CallCount, TsrId, FormName, CampeignID )
        SELECT Contacts.ContactID, Contacts.TelCell, [CampeignID], '0', '0', '0', '76', '10'
        FROM Contacts
        WHERE Contacts.TelCell Like '0%' 
        AND Contacts.LeadResultID Is Null 
        AND Contacts.CampeignID = '{CampeignID}'
        AND CAST(Import_Date as Date) = CAST(GETDATE() as Date);
    """
    return sql


