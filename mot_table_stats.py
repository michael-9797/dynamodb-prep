import boto3
import json
from dateutil.relativedelta import relativedelta
from datetime import datetime
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
import logging

def extract_number(record):


    reg   = record["REG"]
    date  = record["DATE"]
    phone = record["PHONE NUMBER"]
    
    extracted = (reg, date, phone)
    
    return extracted

def empty_num_check(record):
    
    return record["PHONE NUMBER"] == "" 

# scan dynamo db
def mot_scan(start, end, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('MOT')
    
    filter_expression = Attr('DATE').between(start,end)
    response = table.scan(
        FilterExpression=filter_expression
        )
    customers = response["Items"]
    while "LastEvaluatedKey" in response:
        response = table.scan(
            FilterExpression=filter_expression,
            ExclusiveStartKey=response["LastEvaluatedKey"]
            )
        customers.extend(response["Items"])
    return customers
    
def mot_empty_numbers(start, end, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('MOT')
    
    filter_expression = Attr('DATE').between(start,end)
    
    res = []
    response = table.scan(
        FilterExpression=filter_expression
        )
        
       
    customers = map(extract_number,filter(empty_num_check,response["Items"]))
    res.extend(list(customers))
    
    while "LastEvaluatedKey" in response:
        response = table.scan(
            FilterExpression=filter_expression,
            ExclusiveStartKey=response["LastEvaluatedKey"]
            )
        customers = map(extract_number,filter(empty_num_check,response["Items"]))
        res.extend(list(customers))
        
    return res
    
    
if __name__ == "__main__":
    
    
    logging.getLogger().setLevel(logging.INFO)
    
    start = "2021-01-01"
    end   = "2021-12-31"
    
    empty_records = mot_empty_numbers(start, end)
    records = mot_scan(start, end)
    
    logging.info("found %d records between %s and %s",len(records),start,end)
    logging.info("found %d records with empty PHONE NUMBER between %s and %s",len(empty_records),start,end)
