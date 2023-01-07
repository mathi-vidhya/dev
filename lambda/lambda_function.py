import json
import time
import consonants as constant
import pytz
import datetime
from datetime import timedelta
from pytz import timezone
from pandas import json_normalize
import requests
# import pyarrow
import boto3

import pandas as pd
from pandas import DataFrame

from io import BytesIO
from io import StringIO
from json import dumps
from pandas.io.json import json_normalize
from datetime import datetime as dt
bucket = 'data-mathi1' # already created on S3
client = boto3.client('ssm')
sns = boto3.client("sns", region_name="ap-northeast-1")
ssm = boto3.client("ssm", region_name="ap-northeast-1")
s3_resource = boto3.resource('s3')
# url = ssm.get_parameter(Name=constant.urlapi, WithDecryption=True)["Parameter"]["Value"]
url = "https://results.us.securityeducation.com/api/reporting/v0.1.0/phishing"
# my_headers =ssm.get_parameter(Name=constant.tokenapi, WithDecryption=True)["Parameter"]["Value"] 
header1={'x-apikey-token' : 'YV5iYWWji13z25LX/ZAQOwmHKmHqdpQk8pQzIcSQ5hGl3cpog'}
s3_prefix = "result/csvfiles"
def get_datetime():
    dt = datetime.datetime.now()
    return dt.strftime("%Y%m%d"), dt.strftime("%H:%M:%S")
datestr, timestr = get_datetime()
fname = f"data_api_tripler_{datestr}_{timestr}.csv"
file_prefix = "/".join([s3_prefix, fname])
print(file_prefix)

def send_sns_success():
    success_sns_arn = ssm.get_parameter(Name=constant.SUCCESSNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
    component_name = constant.COMPONENT_NAME
    env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    success_msg = constant.SUCCESS_MSG
    sns_message = (f"{component_name} :  {success_msg}")
    print(sns_message, 'text')
    succ_response = sns.publish(TargetArn=success_sns_arn,Message=json.dumps({'default': json.dumps(sns_message)}),
        Subject= env + " : " + component_name,MessageStructure="json")
    return succ_response
    
def send_error_sns():
   
    error_sns_arn = ssm.get_parameter(Name=constant.ERRORNOTIFICATIONARN)["Parameter"]["Value"]
    env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    error_message=constant.ERROR_MSG
    component_name = constant.COMPONENT_NAME
    sns_message = (f"{component_name} : {error_message}")
    err_response = sns.publish(TargetArn=error_sns_arn,Message=json.dumps({'default': json.dumps(sns_message)}),    Subject=env + " : " + component_name,
        MessageStructure="json")
    return err_response

def lambda_handler(event, context):
    
    try:
        r=requests.get(url,headers=header1)
        d=r.json()
        df0=pd.DataFrame(d["data"])
        df1=df0[df0.columns.drop(['attributes'])]
        a=[d.values() for d in df0.attributes]
        b=[d.keys() for d in df0.attributes]
        x=b.pop()
        df2=pd.DataFrame(a,columns=x)
        def add(x):
            if x == "E01272":
                return "fail"
            elif x == "E76763":
                return "pass"
            else:
                return "pending"
        df2["Status"]=df2["sso_id"].apply(add)
        df=pd.concat([df1,df2], axis=1, join="outer")
        df['date'] = pd.to_datetime('today').strftime("%y/%m/%d")
        print(df)
        csv_buffer1 = StringIO()
        df.to_csv(csv_buffer1)
        # Writing the Files to CSV 
        s3_resource.Object(bucket, file_prefix).put(Body=csv_buffer1.getvalue())
        print('CSV files written')
      ####SUCCESS-SNS-NOTIFICATION ACTIVATED#####    
        send_sns_success()
        print('Email delivered')
    except Exception as e:
        error_message=str(e)
        print(error_message)
        send_error_sns("SYSTEM_ERRORS", "TECHNICAL_ERROR", "LAMBDA_ERRORS", error_message=str(e))
#     # TODO implement
    return {
            'statusCode': 200,
        'body': json.dumps('Hurry!!! ..Dataframe Created and loaded!')
}