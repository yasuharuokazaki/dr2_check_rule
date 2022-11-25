import sys
import pandas as pd
from classes import CheckList, CheckRule, TriggerRow, TriggerTable
from functions import list_filter,trigger_pickup
from dataclasses import dataclass
from typing import List
from dataclasses import replace
import re
import itertools
import boto3
from dotenv import load_dotenv
import os
import openpyxl
import requests
from requests_aws4auth import AWS4Auth
import io
import json
import create_trigger
import urllib
from datetime import datetime
load_dotenv()

s3 = boto3.resource('s3',region_name='ap-northeast-1')
s3_client = boto3.client('s3',region_name='ap-northeast-1')
dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-1')


def handler(event,context):
    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = urllib.parse.unquote_plus(event["Records"][0]['s3']['object']['key'], encoding='utf-8')
    csv_path = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key']).split('/')[:-2]
    trigger_table = makeTriggerTable(event)
    check_rule = makeCheckRule(bucket,csv_path)
    fs1_result = fs1_pickup(trigger_table,check_rule)
    fs2_result = fs2_pickup(trigger_table,check_rule)
    pj_result = pj_pickup(trigger_table,check_rule)
    ot_result = ot_pickup(trigger_table,check_rule)
    in_result = in_pickup(trigger_table,check_rule)

    full_result = set(fs1_result+fs2_result+pj_result+ot_result+in_result)
    read_list = list(full_result)

    df=pd.DataFrame(read_list)
    df['FB']='-'
    rename_df=df.rename(columns={0:'name'})
    project = re.split('/',s3_key)[-2]
    FILE_PATH='/tmp/{}_check_need_triggers.xlsx'.format(project)
    rename_df.to_excel(FILE_PATH)
    repo=re.split('/',s3_key)[0]
    cognito_id = re.split('/',s3_key)[1]
    upload_path = f"{repo}/{cognito_id}/dr2_trigger/{project}_check_need_triggers.xlsx"
    print(f'upload path:{upload_path}')
    s3_client.upload_file(FILE_PATH,bucket,upload_path)
    print(f"save as {project}")

    fileName = re.split('/',s3_key)[-1]
    ID = re.split('_',fileName)[0]
    table = dynamodb.Table('DrList-xomvxskiujbz3i7mwltiybanjy-dev')
    table.update_item(
        Key={
            'id':ID
        },
        UpdateExpression="SET dr2_status = :val1",
        ExpressionAttributeValues={
            ':val1':upload_path
        }
    )
    print('Done All')

  
    # extractTriggers(event)
 


def makeTriggerTable(event):
    bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = urllib.parse.unquote_plus(event["Records"][0]['s3']['object']['key'], encoding='utf-8')
    obj = s3.Object(bucket,s3_key).get()
    trigger_csv = io.TextIOWrapper(io.BytesIO(obj['Body'].read()))
    csv_df = pd.read_csv(trigger_csv)
    
    indexs =csv_df.iloc[:,0]
    fixed_indexs = [re.sub('-(.*\d)-','-0\\1-',index) for index in indexs]
    csv_df['index']=fixed_indexs
    
    tmp_path = '/tmp/tmp_file_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S-')+"correspondence_table.xlsm"
    csv_path = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key']).split('/')[:-2]
    s3_correspondence_key=f"{csv_path[0]}/{csv_path[1]}/correspondence_table/trigger_table-2.xlsm"
    print('s3Key',s3_correspondence_key)
    s3_client.download_file(bucket,s3_correspondence_key,tmp_path)

    book = pd.ExcelFile(tmp_path)
    book_sheet_names = book.sheet_names
    sheet_df = book.parse(book_sheet_names[1])
    contents_df = sheet_df.iloc[:,:2]
    items = [item['項目'] for n,item in contents_df.iterrows() for csv in csv_df['index'] if item['項目index']==csv]
    csv_df['title']=items
    
    #カラム並び替え
    reindexed_df = csv_df.reindex(columns=['index','title','value'])
    cols_rename_df = reindexed_df.rename(columns={'index':'項目index','title':'項目','value':'内容'})
    trigger_table =TriggerTable(cols_rename_df)
    return trigger_table

def makeCheckRule(bucket,csv_path):
    #チェックルール読み込み
    s3_key_rule = f"{csv_path[0]}/{csv_path[1]}/rule/rule_base-2.xlsx"
    tmp_rule_path = '/tmp/tmp_file_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S-')+"rule_base.xlsx"
    s3_client.download_file(bucket,s3_key_rule,tmp_rule_path)


    #rule sheetを
    rule_book = pd.ExcelFile(tmp_rule_path)
    sheet_names_ls = rule_book.sheet_names
    rule_sheet = rule_book.parse(sheet_names_ls[4])
    check_list = rule_sheet.iloc[7:,10]
    conditions = rule_sheet.iloc[7:,14:19]

    #必要な情報だけ抜き出したDF
    df = pd.concat([check_list,conditions],axis=1)
    df.columns = ["失敗に学ぶ","FS1","FS2","PJ","OT","IN"]
    check_list = CheckList(df)
    print('チェックルール読み込み完了')
    return check_list

def fs1_pickup(trigger_table:TriggerTable,rule_table:CheckList):
    rules = rule_table.check_rules
    return_triggers = {}
    for rule in rules[1:]:
        return_triggers[rule.name]=[]
        if(rule.fs1_condition!='ー' and str(rule.fs1_condition)!= 'nan'):
            conditions = [item.replace('\n',"") if '\n' in str(item) else item for item in re.split("：",rule.fs1_condition)]
            hit_triggers = [trigger for trigger in trigger_table.fs1_rows for condition in conditions 
                            if condition in str(trigger.title) and 
                            trigger.description!='0' and trigger.description != ""
                            and str(trigger.description) != 'nan']
            return_triggers[rule.name] += hit_triggers
    return [result[0] for result in return_triggers.items() if result[1] != []]

def fs2_pickup(trigger_table:TriggerTable,rule_table:CheckList):
    rules = rule_table.check_rules
    return_triggers = {}
    for rule in rules[1:]:
        return_triggers[rule.name]=[]
        if(rule.fs2_condition != 'ー' and str(rule.fs2_condition)!='nan'):
            conditions = [item.replace('\n',"") if '\n' in str(item) else item for item in re.split("）",rule.fs2_condition)]
            remove_prefix_conditions = conditions[-1]
            hit_triggers = [trigger for trigger in trigger_table.fs2_rows for condition in remove_prefix_conditions 
                            if condition in str(trigger.title) and trigger.description!='0' and trigger.description != ""
                            and str(trigger.description) != 'nan']
            return_triggers[rule.name] += hit_triggers
    return [result[0] for result in return_triggers.items() if result[1] != []]

def pj_pickup(trigger_table:TriggerTable,rule_table:CheckList):
    rules = rule_table.check_rules
    return_triggers = {}
    for rule in rules[1:]:
        return_triggers[rule.name]=[]
        if(rule.pj_condition != 'ー' and str(rule.pj_condition)!='nan'):
            if('or' in str(rule.pj_condition)):
                or_conditions = re.split('or',rule.pj_condition)
                remove_space_or_conditions = [condition.replace(" ",'').replace('\u3000','') for condition in or_conditions]
                filled_conditions = [condition for condition in remove_space_or_conditions for trigger in trigger_table.pj_rows
                                     if condition in str(trigger.description)]
                if(len(filled_conditions)>0):
                    return_triggers[rule.name] += [trigger for trigger in trigger_table.pj_rows for condition in filled_conditions 
                                                    if condition in str(trigger.title) and trigger.description!='0' and trigger.description != ""
                                                    and str(trigger.description) != 'nan']
            elif('AND' in str(rule.pj_condition)):
                and_conditions = re.split('AND',rule.pj_condition)
                remove_space_and_conditions = [condition.replace(" ",'').replace('\u3000','') for condition in and_conditions]
                filled_conditions = [condition for condition in remove_space_and_conditions for trigger in trigger_table.pj_rows
                                     if condition in str(trigger.description)]
                if(len(filled_conditions)>=len(and_conditions)):
                    return_triggers[rule.name] += [trigger for trigger in trigger_table.pj_rows for condition in filled_conditions 
                                                    if condition in str(trigger.title) and trigger.description!='0' and trigger.description != ""
                                                    and str(trigger.description) != 'nan']
            else:
                return_triggers[rule.name] += [trigger for trigger in trigger_table.pj_rows for condition in filled_conditions 
                                                    if condition in str(trigger.title) and trigger.description!='0' and trigger.description != ""
                                                    and str(trigger.description) != 'nan']
    return [result[0] for result in return_triggers.items() if result[1] != []]

def roop(trigger:list,list:list,trigger_rows:list):
        for i in list:
            if("and" in i):
                and_conditions = re.split('and',i)
                roop(trigger,and_conditions,trigger_rows)
            elif('or' in i):
                or_conditions = re.split('or',i)
                roop(trigger,or_conditions,trigger_rows)
            else:
                trigger += [trigger_row for trigger_row in trigger_rows if i in str(trigger_row.description)]
        return trigger

#flatten
def flatten_list(l):
    for el in l:
        if isinstance(el, list):
            yield from flatten_list(el)
        else:
            yield el

# OTについて
def ot_pickup(trigger_table:TriggerTable,rule_table:CheckList):
    rules = rule_table.check_rules
    condition_dict={}
    return_triggers = {}
    trigger_rows = trigger_table.ot_rows
    for rule in rules[1:]:
        return_triggers[rule.name]=[]
        if(rule.ot_condition != 'ー' and str(rule.ot_condition)!='nan'):
            splitted_conditions = [condition.replace('\u3000','').replace('\n','').replace(" ",'') for condition in re.split("：|:",str(rule.ot_condition))]
            ab_splitted_conditions = [re.split('〈',c.replace('〉','')) if '〈' in str(c) else c for c in splitted_conditions]
            flatten_conditions = list(flatten_list(ab_splitted_conditions))
            results = roop([],flatten_conditions,trigger_rows)
            for condition in flatten_conditions:
                if('and' in condition):
                    and_conditions = re.split('and',condition)
                    hit_triggers = [result for result in results for condition in and_conditions if condition in result.description]
                    if(len(and_conditions)==len(hit_triggers)):
                        return_triggers[rule.name] = hit_triggers
                elif('or' in condition):
                    or_conditions = re.split('or',condition)
                    hit_triggers = [result for result in results for condition in or_conditions if condition in result.description]
                    if(len(hit_triggers)>0):
                        return_triggers[rule.name] = hit_triggers
                elif(condition==flatten_conditions[-1]):
                    return_triggers[rule.name] = [result for result in results if condition in result.description]
                else:
                    continue
    return [result[0] for result in return_triggers.items() if result[1] != []]

def in_pickup(trigger_table:TriggerTable,rule_table:CheckList):
    rules = rule_table.in_conditions()
    condition_dict={}
    return_triggers = {}
    trigger_rows = trigger_table.in_rows
    for rule in rules[1:]:
        return_triggers[rule.name]=[]
        if(rule.in_condition != 'ー' and str(rule.in_condition)!='nan'):
            #checkルールをOR又はAndで分ける
            if('and' in rule.in_condition or 'AND' in rule.in_condition):
                and_splitted_conditions=re.split('and|AND',str(rule.in_condition))
                and_conditions = [condition.replace(" ",'') for condition in and_splitted_conditions]
                condition_num = len(and_conditions)
                splitted_and_conditions = [re.split("：|:",condition) for condition in and_conditions]
                condition_list = list(flatten_list(splitted_and_conditions))
                result = [trigger_row for trigger_row in trigger_rows for condition in condition_list if condition in str(trigger_row.description)]
                if(len(result)==condition_num):
                    return_triggers[rule.name] = result
            elif('or' in rule.in_condition or 'OR' in rule.in_condition):
                or_splitted_conditions=re.split('or|OR',str(rule.in_condition))
                or_conditions = [condition.replace(" ",'').replace('\u3000','').replace('\n','') for condition in or_splitted_conditions]
                condition_num = len(or_conditions)
                splitted_or_conditions = [re.split("：|:",condition) for condition in or_conditions]
                condition_list = list(flatten_list(splitted_or_conditions))
                result = [trigger_row for trigger_row in trigger_rows for condition in condition_list if condition in str(trigger_row.description)]
                if(len(result)>0):
                    return_triggers[rule.name] = result
            else:
                #:を含んでいたら、
                conditions = re.split("：|:",rule.in_condition)
                result = [trigger_row for trigger_row in trigger_rows for condition in conditions if condition in str(trigger_row.description)]
                return_triggers[rule.name] = result
    return [result[0] for result in return_triggers.items() if result[1] != []]