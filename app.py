import pandas as pd
import requests as r
import time
from datetime import datetime
from Akvo import Flow

host="http://localhost/api/"
auth = r.auth.HTTPBasicAuth('admin','district')

INSTANCE = "INSTANCE"
SURVEYID = "SURVEYID"
FORMID = "FORMID"
DHIS_AKVO = "DHIS_SECRETS"
dataPoints = []
start_time = time.time()

requestURI = "https://api.akvo.org/flow/orgs/" + INSTANCE
surveyURI = "https://api.akvo.org/flow/orgs/" + INSTANCE + "/surveys/" + SURVEYID
formURI = "https://api.akvo.org/flow/orgs/" + INSTANCE + "/form_instances?survey_id=" + SURVEYID + "&form_id=" + FORMID#f

class Dhis2:
    def __init__(self,path):
        api = host+path
        data = r.get(api,auth=auth).json()
        if "/" not in path:
            self.lists = data[path]
        self.page = data
        self.detail = data
class Events:
    def __init__(self,path,data):
        api = host+path
        data = r.get(api,auth=auth,params=data).json()
        self.detail = data
class Capture:
    def __init__(self,path,data):
        api = host+path
        req = r.post(api,auth=auth,json=data).json()
        self.response = req
class Program:
    def __init__(self,ids):
        api = host+"programs/"+ids
        req = r.get(api,auth=auth).json()
        self.detail = req
class ProgramStage:
    def __init__(self,ids):
        api = host+"programStages/"+ids
        req = r.get(api,auth=auth).json()
        self.dataElement = req["programStageDataElements"]


def Transform(data, qType):
    try:
        if (data == 'Error'):
            return ""
        elif(qType=='OPTION'):
            return handleOption(data)
        elif(qType=='PHOTO'):
            return handlePhotoQuestion(data)
        elif(qType=='CADDISFLY'):
            return handleCaddisfly(data)
        elif(qType=='VIDEO'):
            return handleVideoQuestion(data)
        elif(qType=='GEOSHAPE'):
            return handleGeoshape(data)
        elif(qType=='GEO'):
            return handleGeolocation(data)
        elif(qType=='FREE_TEXT'):
            return handleFreeText(data)
        elif(qType=='SCAN'):
            return handleBarCode(data)
        elif(qType=='DATE'):
            return handleDate(data)
        elif(qType=='NUMBER'):
            return handleNumber(data)
        elif(qType=='CASCADE'):
            return handleCascade(data)
        elif(qType=='SIGNATURE'):
            return handleSignature(data)
        else:
            return ""
    except:
        data = None
    return data

def handleOption(data):
    response=""
    for value in data:
        if(response==""):
            if(value.get("code")==None):
                response=value.get('text',"")
            else :
                response=value.get('code')+":"+value.get('text',"")
        elif(response):
            if(value.get("code")==None):
                response=response+"|"+value.get('text',"")
            else:
                response=response+"|"+value.get('code',"")+":"+value.get('text',"")
    return response

def handleFreeText(data):
    return data

def handleBarCode(data):
    return data

def handleDate(data):
    return data

def handleNumber(data):
    return int(data)

def handleCascade(data):
    response=""
    for value in data:
        if(response==""):
            if(value.get("code")==None):
                response=value.get('name',"")
            else:
                response=value.get('code',"")+":"+value.get("name","")
        elif(response):
            if(value.get("code")==None):
                response=response+"|"+value.get('name',"")
            else:
                response=response+"|"+value.get('code',"")+":"+value.get("name","")

    return response

def handleGeoshape(data):
    return data

def handleGeolocation(data):
    response = []
    response.append(round(data.get('lat'),6))
    response.append(round(data.get('long'),6))
    strings = str(response).replace(" ","")
    return strings

def handleCaddisfly(data):
    return data

def handlePhotoQuestion(data):
    return data.get('filename',"")

def handleVideoQuestion(data):
    return data.get('filename',"")

def handleSignature(data):
    return data.get('name',"")

def checkTime(x):
    total_time = x - start_time
    spent = time.strftime("%H:%M:%S", time.gmtime(total_time))
    return spent

def getAll(url):
    data = Flow.getData(url,Flow.getToken())
    formInstances = data.get('formInstances')
    for dataPoint in formInstances:
        dataPoints.append(dataPoint)
    try:
        print(checkTime(time.time()) + ' GET DATA FROM[' + INSTANCE + "," + url.split("?")[1].replace("&",",") + ']')
        url = data.get('nextPageUrl')
        getAll(url)
    except:
        print(checkTime(time.time()) + ' DOWNLOAD COMPLETE')
        return "done"

apiData = Flow.getData(surveyURI,Flow.getToken()).get("forms")
questions = lambda x : [{'id':a['id'],'name':a['name'],'questions':details(a['questions'])} for a in x]
details = lambda x : [{'id':a['id'],'name':a['name'].replace(' ','_'),'type':a['type'],'code':a['variableName']} for a in x]
meta = questions(apiData[0]['questionGroups'])
mt = pd.DataFrame(meta)
groupID = mt['id'][0]
metadata = mt['questions'][0]
metacode = lambda x:{y["id"]:y["code"] for y in x}
getAll(formURI)
output = pd.DataFrame(dataPoints)

# Checking if Available

eventParams = {
    "orgUnit": "ImspTQPwCqd",
    "programStage": "pSllsjpfLH2",
    "program":"q04UBOqq3rp",
}
StoredEvents = Events("events",eventParams).detail["events"]
storedValues = lambda x:[y["dataValues"] for y in x]
StoredEvents = storedValues(StoredEvents)
for se in StoredEvents:
    for el in se:
        if el["dataElement"] == DHIS_AKVO:
            output = output[output.identifier != el["value"]]

def transform(data):
    values = []
    for mt in metadata:
        for dt in data[0]:
            value = Transform(dt[mt["id"]],mt["type"])
            if mt["code"] == None:
                pass
            else:
                values.append({"dataElement":mt["code"],"value":value})
    values.append({"dataElement": DHIS_AKVO, 'value': data[1]})
    return values

def insertData(output):
    output["responses"] = output["responses"].apply(lambda x:x[groupID])
    output["dataValues"] = output[['responses', 'identifier']].apply(lambda x: transform(x), axis=1)
    output["program"] = "q04UBOqq3rp"
    output["programStage"] = "pSllsjpfLH2"
    output["orgUnit"] = "ImspTQPwCqd"
    output["status"] = "COMPLETED"
    output["eventDate"] = output["submissionDate"].apply(lambda x:datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d'))
    output["completedDate"] = output["eventDate"]
    removecol = [
        "dataPointId",
        "identifier",
        "formId",
        "id",
        "responses",
        "createdAt",
        "displayName",
        "modifiedAt",
        "submitter",
        "surveyalTime",
        "deviceIdentifier",
        "submissionDate"
    ]
    output = output.drop(columns=removecol)
    inputValues = output.to_dict("records")
    for values in inputValues:
        print(checkTime(time.time()) + ' PUSHING NEW DATA: '+ values["dataValues"][3]["value"])
        Capture("events",values).response

if len(output) == 0:
    print(checkTime(time.time()) + ' NEW DATA NOT FOUND!')
else:
    print(checkTime(time.time()) + ' NEW DATA FOUND! TRANSFORMING...')
    insertData(output)
print(checkTime(time.time()) + ' JOB IS DONE...')

