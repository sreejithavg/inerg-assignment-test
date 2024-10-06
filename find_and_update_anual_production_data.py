from email import message
import pandas as pd 
import os 
from sqliteConnect import SqliteDB
def getDataFromSheet():
    cp = os.getcwd()
    xls_path = cp + "\\20210309_2020_1 - 4 (1) (1) (1).xls"
    # print(xls_path)
    df = pd.read_excel(xls_path)
    return df

def calculateAnualProduction(df):
    grouped_df = df.groupby("API WELL  NUMBER")
    anual_productions = []
    j=0
    # print(grouped_df.size())
    excepted = False
    for name , group in grouped_df:
        anual_production = {
            "OIL" : 0,
            "GAS":0,
            "BRINE":0
        }
        anual_production['API WELL  NUMBER'] = name
        for index, df_item in group.iterrows():
            try:
                anual_production['OIL'] += df_item['OIL']
                anual_production['GAS'] += df_item['GAS']
                anual_production['BRINE'] += df_item['BRINE']
            except Exception as err :
                print(str(err))
                # print (df_item)
                excepted = True
        if excepted:
            break
        anual_productions.append(anual_production)
    return anual_productions

def writeToDB(data_to_Write):
    db_connect = SqliteDB()
    data_tuple =  [(anual_data['API WELL  NUMBER'], anual_data['OIL'],anual_data['GAS'], anual_data['BRINE'] ) for anual_data in data_to_Write]
    excecute_query = f"INSERT INTO AnualProduction (ID,OIL,GAS,BRINE ) VALUES (?, ?,?,?)"
    status,message = db_connect.insert_users_bulk(excecute_query,data_tuple)
    print(message)
    db_connect.close()

def updateAnualDataToDB():
    df = getDataFromSheet()
    anual_production_data = calculateAnualProduction(df)
    writeToDB(anual_production_data)

updateAnualDataToDB()
# db_connect = SqliteDB()
# db_connect.drop_table("AnualProduction")
