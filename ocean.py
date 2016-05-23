from bson import json_util
from bson import objectid
from flask import Flask
from flask import jsonify
from flask import request
import json
import pymongo
from pymongo import MongoClient
import re

app = Flask(__name__)
app.config['PROPAGATE_EXCEPTIONS'] = True

@app.route("/ocean/eddies")
def eddies():
    client = MongoClient()
    db = client.ocean
    result = db.ocean.find()
    return str(json.dumps({'results': list(result)}, default=json_util.default))

@app.route("/ocean/eddies/eddy/<eddyId>")
def oneEddy(eddyId):
    client = MongoClient()
    db = client.ocean
    result = db.ocean.find({'_id': objectid.ObjectId(eddyId)})
    return str(json.dumps({'results': list(result)}, default=json_util.default))
    #return jsonify()

@app.route("/ocean/eddies/near")
def near():
    client = MongoClient()
    db = client.ocean
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    result = db.ocean.find({["geometry"]["coordinates"]: {"$near": [lon, lat]}})
    return str(json.dumps({'result': list(result)}, default=json_util.default))

@app.route("/ocean/eddies/type/near/<type>")
def typeNear(type):
    client = MongoClient()
    db = client.ocean
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    myregex = re.compile(type, re.I)
    # Lingstring/Polygon
    result = db.ocean.find({["geometry"]["type"]: myregex, ["geometry"]["coordinates"]: {"$near": [lon, lat]}})
    return str(json.dump({'result': list(result)}, default=json_util.default))

@app.route("/test")
def test():
    return "The application is working now!"

if __name__ == "__main__":
    app.run()
