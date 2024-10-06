from flask import Flask,jsonify,request
from sqliteConnect import SqliteDB
app = Flask(__name__)
@app.route('/anual_data/<int:well_number>', methods=['GET'])
def fetchDataByWellNumber(well_number):
    try:
        fetch_query = "SELECT * FROM AnualProduction WHERE ID = ?"
        db_connc = SqliteDB()
        production_data = db_connc.fetchOneData(fetch_query,(well_number,))
        if production_data:
            print(production_data)
            anual_production_data = {
                "OIL": production_data["OIL"],
                "GAS" : production_data["GAS"],
                "BRINE" : production_data["BRINE"],
            }
            response = { 
                "status_code":200,
                "status_desc":"successfully fetched the data.",
                "status":"success",
                "data":anual_production_data
            }
        
        else:
            response = { 
                "status_code":400,
                "status_desc":"No matching data found",
                "status":"Failed"
            }
    except Exception as err :
        response = { 
                "status_code":500,
                "status_desc":str(err),
                "status":"Failed",
            }
    return jsonify(response)

if __name__ == '__main__':
    app.run(port="8001",debug=True)