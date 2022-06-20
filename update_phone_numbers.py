import boto3
import json
from dateutil.relativedelta import relativedelta
from datetime import datetime
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
import logging

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

#Query dynamodb over a range of dates
def query_on_date(dates, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('MOT')
    
    #Get items 
    customers = []
    
    for date in dates:
        query_kwargs = {
        "IndexName":"DATE-index",
        "KeyConditionExpression": Key('DATE').eq(date)
        }
    
        response = table.query(
                     **query_kwargs
                )
         
        customers.extend(response["Items"])
        
        while "LastEvaluatedKey" in response:
            query_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            response = table.query(
                     **query_kwargs
                     )
            customers.extend(response["Items"])

    return customers

#Query dynamodb over a range of dates
def query_on_reg(reg, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('MOT')
    
    #Get items 
    customers = []
    
    query_kwargs = {
    "KeyConditionExpression": Key('REG').eq(reg),
    "ScanIndexForward":False
    }

    response = table.query(
                 **query_kwargs
            )
     
    customers.extend(response["Items"])
    
    while "LastEvaluatedKey" in response:
        query_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        response = table.query(
                 **query_kwargs
                 )
        customers.extend(response["Items"])

    return customers
    
def extract_number(record):


    reg   = record["REG"]
    date  = record["DATE"]
    phone = record["PHONE NUMBER"]
    
    extracted = (reg, date, phone)
    
    return extracted

def empty_num_check(record):
    
    return record["PHONE NUMBER"] == "" 

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

def update_item_on_value(partition_key,sort_key, update_column, update_value, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('MOT')
    
    response = table.update_item(
    Key={
        'REG': partition_key,
        'DATE': sort_key
    },
    UpdateExpression=f'SET #NUM = :newNumber',
    ExpressionAttributeNames={
        "#NUM":f"{update_column}"
    },
    ExpressionAttributeValues={
        ':newNumber': update_value,
        
    },
    ReturnValues="UPDATED_NEW"
    )
        
    return response




if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)
    
    start = "2021-01-01"
    end   = "2021-12-31"
    
    empty_records = mot_empty_numbers(start, end)
    records = mot_scan(start, end)
    
    logging.info("found %d records with empty PHONE NUMBER between %s and %s",len(empty_records),start,end)
    logging.info("found %d records between %s and %s",len(records),start,end)
    
    #eg = empty_records[0][0]
    #res = query_on_reg(eg)
    #logging.info("Records for REG %s are:\n%s",eg,res)
    
    records_to_update = []
    
    for reg, date, number in empty_records:
        record_history = query_on_reg(reg)[:4]
        for record in record_history:
            if record["PHONE NUMBER"]:
                to_update = (reg, date, record["PHONE NUMBER"])
                records_to_update.append(to_update)
                break
                
    
    logging.info("found %d empty PHONE NUMBER records to update between %s  and %s",len(records_to_update),start,end)    
    logging.info("about to update records ")
    
    for record in records_to_update:
        partition_key,sort_key, update_value = record
        result =  update_item_on_value(partition_key,sort_key,"PHONE NUMBER", update_value)
        logging.info("updated %s ",result)
    
    
    
    