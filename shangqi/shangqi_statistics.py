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
    projects_info = {}
    results = r.json()["results"]
    # print(json.dumps(results))
    for i in range(len(results)):
        # print("😂",results[i]["properties"]["项目名"])
        try:
            key = results[i]["properties"]["项目名"][col_type(results,i,"项目名")][0]["plain_text"]
            value = results[i]["properties"][col_name][col_type(results,i,col_name)][0]["plain_text"]
            type = results[i]["properties"]["项目类型"][col_type(results,i,"项目类型")]["name"]
            print(results[i]["properties"]["项目类型"])
            if key not in projects_info.keys():
                projects_info[key] = [list(map(int,re.findall('[0-9]+',value) )),type] #只取数字，其他都不要
                # break
            else:
                print("😂")
        except:
            print("🐽") # 只取有效的行，有些空的没填完整的就跳过
    return projects_info
    # print(projects,len(projects))


def get_result_from_hasura(url,pwd,query,variables):
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



def set_variables(pool_ids,payload_variables_structure,start,end):
    # 设置 variable
    payload_variables_structure["start_time"] = start
    payload_variables_structure["end_time"] = end
    payload_variables_structure["pool_ids"] = pool_ids
    return payload_variables_structure



def write_csv(file,sheet_name,data):
    if not os.path.exists(file):
        with pd.ExcelWriter(path=file, mode="w",engine="openpyxl") as writer:
                data.to_excel(writer,sheet_name=sheet_name)
    else:
        with pd.ExcelWriter(path=file, mode="a",engine="openpyxl") as writer:
                data.to_excel(writer,sheet_name=sheet_name)

def auth(file):
    with open(file,'r') as f:
        obj = json.load(f)
        url = obj["url"]
        pwd = obj["x-hasura-admin-secret"]
        token = obj["token"]
    return url,pwd,token


def run(auth_file,table_url,col_name,start,end,hasura_query,hasura_variables,to_file,to_sheet="sheet-1"):
    results = {"项目名称":[],"数量":[],"项目类型":[],}
    hasura_url,hasura_pwd,notion_token = auth(auth_file)  #读取token之类的东西
    notion_results = read_table(table_url,notion_token,col_name=col_name) # 从 notion 读取必要数据
    for k,v in notion_results.items():
        hasura_variables["start_time"] = start
        hasura_variables["end_time"] = end
        hasura_variables["pool_ids"] = v[0]
        r = get_result_from_hasura(url=hasura_url,pwd=hasura_pwd,query=hasura_query,variables=hasura_variables)  # 请求 hasura
        print(r)
        count = list(r["data"].values())[0]['aggregate']['count']
        results["项目名称"].append(k)
        results["数量"].append(count)
        results["项目类型"].append(v[1])
    write_csv(file=to_file,sheet_name=to_sheet,data=pd.DataFrame(results))


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
    s_e_p_variables =     {
            "start_time": "2020-9-18 8:00:00",
            "end_time": "2021-9-24 17:00:00",
            "pool_ids":
            [
                41930,
                35321
            ]
        }
    

    start_time =  "2021-10-11 17:00:00"
    end_time =  "2021-10-12 20:00:00"
    confirm_msg = "起始时间为：{}       截止时间为:{}\n".format(start_time,end_time)
    confirm_choice(confirm_msg) #confirm
    auth_file = "/Users/lizhe/Desktop/shangqi-hasura.json"   # 存放 url、pwd 和 token 的 json
    target_table_url = "https://api.notion.com/v1/databases/3d40984aec444edaa74d1d2dbc4402b8/query"
    to = "shangqi-{type}.xls"


    file_1 = to.format(type="annotated",start=start_time,end=end_time)
    file_2 = to.format(type="accepted",start=start_time,end=end_time)
    run(auth_file=auth_file,table_url=target_table_url,col_name="标注池",start=start_time,end=end_time,hasura_query=query_of_accepted_data_between_two_times,hasura_variables=s_e_p_variables,to_file=file_1)

    # sender_email = "zhe.li@stardust.ai"
    # receiver_email = "zhe.li@stardust.ai"
    # text = """This is an automated email message:\n===========================\n 以下为上汽项目 {start} 到 {end} 的统计数据\n===========================\n\n\n===========================\n\n""".format(start=start_time,end=end_time)
    # send_email([file_1,file_2],sender_email,receiver_email,text=text)
    # os.remove(file_1)
    # os.remove(file_2)


    # webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ae54da2e-809f-47bf-90cc-d10c9a0a27da" #测试
    # send_notification()   
