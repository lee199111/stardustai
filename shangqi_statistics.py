from typing import Type
import requests
import json
import re
import pandas as pd
from send_email import send_email
from wechat_bot import send_notification
import os

def confirm_choice(msg):
    confirm = input("[c]Confirm: {}".format(msg))
    if confirm != 'c' and confirm != 'v':
        print("\n Invalid Option. Please Enter a Valid Option.")
        return confirm_choice(msg) 
    # print (confirm)
    return confirm

def col_type(result,row,col_name):
    # print(row,"🐔",results[row]["properties"][col_name]["type"])
    return result[row]["properties"][col_name]["type"]

def read_table(url_read,token,col_name):
    headers = {
        "Authorization": "Bearer {NOTION_KEY}".format(NOTION_KEY=token),
        "Content-Type": "application/json",
        "Notion-Version": "2021-08-16"
    }
    payload = {}
    r = requests.post(url_read,data=json.dumps(payload),headers=headers)
    projects = {}
    results = r.json()["results"]
    # print(json.dumps(results))
    for i in range(len(results)):
        # print("😂",results[i]["properties"]["项目名"])
        try:
            key = results[i]["properties"]["项目名"][col_type(results,i,"项目名")][0]["plain_text"]
            value = results[i]["properties"][col_name][col_type(results,i,col_name)][0]["plain_text"]
            type = results[i]["properties"]["项目类型"][col_type(results,i,"项目类型")]["name"]
            print(results[i]["properties"]["项目类型"])
            if key not in projects.keys():
                projects[key] = [list(map(int,re.findall('[0-9]+',value) )),type] #只取数字，其他都不要
            else:
                print("😂")
        except:
            print("🐽") # 只取有效的行，有些空的没填完整的就跳过
    return projects
    # print(projects,len(projects))


def requst_hasura(url,pwd,query,variables):
    headers = {
    "content-type":"application/json",
    "x-hasura-admin-secret":pwd
    }
    payload = {
        "query": query,
        "variables":variables
    }
    r = requests.post(url,headers=headers,data=json.dumps(payload))  
    return r.json()


def set_variables(project_name,pool_ids,payload_variables_structure,start,end):
    # 设置 variable
    payload_variables_structure["start_time"] = start
    payload_variables_structure["end_time"] = end
    payload_variables_structure["pool_ids"] = pool_ids
    return payload_variables_structure

def get_result(projects_and_pool_ids_and_type,query,payload_variables_structure,to_file):
        results = {"项目名称":[],"数量":[],"项目类型":[],}
        for key,value in projects_and_pool_ids_and_type.items():
            payload_variables = set_variables(project_name=key, pool_ids=value[0], payload_variables_structure=payload_variables_structure,start=start_time,end=end_time)
            r = requst_hasura(url=url,pwd=pwd,query=query,variables=payload_variables)
            print(r)
            count = r["data"]['task_runs_aggregate']['aggregate']['count']
            type = value[1]
            results["项目名称"].append(key)
            results["项目类型"].append(type)
            results["数量"].append(count)
            # print(key,value,count,type)
        with pd.ExcelWriter(path=to_file, mode="w") as writer:
            pd.DataFrame(results).to_excel(writer)




if __name__ == "__main__":
    #统计某个时间段内实际验收数据量
    query_of_accepted_data_between_two_times = """
    query countTaskRunsByTimeAndPoolId($start_time: timestamptz = "", $end_time: timestamptz = "", $pool_ids: [Int!] = 10) {
    task_runs_aggregate(where: {created_at: {_gte: $start_time}, _and: {created_at: {_lte: $end_time}, _and: {pool_id: {_in: $pool_ids}}}}) {
        aggregate {
        count
        }
    }
    }
    """
    #统计某个时间段内的标注或者质检量
    query_of_annotated_or_reviewed_data_between_two_times="""
    query countTaskRunsByTimeAndPoolId($start_time: timestamptz = "", $end_time: timestamptz = "", $pool_ids: [Int!] = 10) {
    task_runs_aggregate(where: {finished_at: {_gte: $start_time}, _and: {finished_at: {_lte: $end_time}, _and: {pool_id: {_in: $pool_ids}}}}) {
        aggregate {
        count
        }
    }
    }
    """
    payload_variables_structure =     {
            "start_time": "2020-9-18 8:00:00",
            "end_time": "2021-9-24 17:00:00",
            "pool_ids":
            [
                41930,
                35321
            ]
        }
    

    start_time =  "2021-10-09 17:00:00"
    end_time =  "2021-10-11 17:00:00"
    confirm_msg = "起始时间为：{}       截止时间为:{}\n".format(start_time,end_time)
    confirm_choice(confirm_msg) #confirm
    file = "/Users/lizhe/Desktop/shangqi-hasura.json"   # 存放 url、pwd 和 token 的 json
    with open(file,'r') as f:
        obj = json.load(f)
        url = obj["url"]
        pwd = obj["x-hasura-admin-secret"]
        token = obj["token"]
    target_table_url = "https://api.notion.com/v1/databases/3d40984aec444edaa74d1d2dbc4402b8/query"
    to = "shangqi-{type}.xls"

    '''统计标注
    '''
    file_1 = to.format(type="annotated",start=start_time,end=end_time)
    file_2 = to.format(type="accepted",start=start_time,end=end_time)
    projects_and_pool_ids_and_type = read_table(target_table_url,token,col_name="标注池ID")
    get_result(projects_and_pool_ids_and_type,
                query=query_of_annotated_or_reviewed_data_between_two_times,
                payload_variables_structure=payload_variables_structure,
                to_file=file_1)
    
    '''统计客户验收
    '''
    projects_and_pool_ids_and_type = read_table(target_table_url,token,col_name="客户抽检池ID")
    print(projects_and_pool_ids_and_type)
    get_result(projects_and_pool_ids_and_type,
                query=query_of_accepted_data_between_two_times,
                payload_variables_structure=payload_variables_structure,
                to_file=file_2)
    
    #统计星尘质检
    # projects_and_pool_ids = read_table(target_table_url,token,col_name="星尘抽检池ID")
    # print(projects_and_pool_ids)
    # get_result(projects_and_pool_ids,
    #             query=query_of_accepted_data_between_two_times,
    #             payload_variables_structure=payload_variables_structure,
    #             to_file=to.format(type="reviewed",start=start_time,end=end_time))

    '''send email
    '''
    sender_email = "zhe.li@stardust.ai"
    receiver_email = "wenjing.zhang@stardust.ai"
    text = """This is an automated email message:\n===========================\n 以下为上汽项目 {start} 到 {end} 的统计数据\n===========================\n\n\n===========================\n\n""".format(start=start_time,end=end_time)
    send_email([file_1,file_2],sender_email,receiver_email,text=text)
    os.remove(file_1)
    os.remove(file_2)


    '''send_notification
    '''
    # webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ae54da2e-809f-47bf-90cc-d10c9a0a27da" #测试
    shangqi_webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=bc595d9b-3028-417e-bf43-2f2dc7e1b9e2"
    notification = """{"msgtype": "text",
  "text": {
    "content": "Hi，@张文静，上汽的每日统计已发送的邮箱，请查收\n邮箱地址：https://mail.google.com/mail/u/0/#""
  }
}"""
    send_notification(msg=notification,webhook=shangqi_webhook)   
