import requests
import json
import re
import csv
# write
"""
payload = {
    "parent":
    {
        "database_id": "{NOTION_DATABASE_ID}".format(NOTION_DATABASE_ID=table_id)
    },
    "properties":
    {
        "title":
        {
            "title":
            [
                {
                    "text":
                    {
                        "content": "Yurts in Big Sur, California"
                    }
                }
            ]
        }
    }
}
headers = {
    "Authorization": "Bearer {NOTION_KEY}".format(NOTION_KEY=token),
    "Content-Type": "application/json",
    "Notion-Version": "2021-08-16"
}
r = requests.post(url_write,data=json.dumps(payload),headers=headers)
print(r)
"""

def col_type(result,row,col_name):
    # print(row,"🐔",results[row]["properties"][col_name]["type"])
    return result[row]["properties"][col_name]["type"]
# read

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
            if key not in projects.keys():
                projects[key] = list(map(int,re.findall('[0-9]+',value) )) #只取数字，其他都不要
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

def get_result(projects_and_pool_ids,query,payload_variables_structure,to_file):
        results = []
        for key,value in projects_and_pool_ids.items():
            payload_variables = set_variables(project_name=key, pool_ids=value, payload_variables_structure=payload_variables_structure,start=start_time,end=end_time)
            r = requst_hasura(url=url,pwd=pwd,query=query,variables=payload_variables)
            count = r["data"]['task_runs_aggregate']['aggregate']['count']
            results.append([key,count])
            print(key,value,count)
        with open(to_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(results)


if __name__ == "__main__":
    #本周实际验收数据量（不去重，星尘提交给上汽抽查池数量，需要运营给到抽检池ID）
    query_of_current_week_accepted_data = """
    query countTaskRunsByTimeAndPoolId($start_time: timestamptz = "", $end_time: timestamptz = "", $pool_ids: [Int!] = 10) {
    task_runs_aggregate(where: {created_at: {_gte: $start_time}, _and: {created_at: {_lte: $end_time}, _and: {pool_id: {_in: $pool_ids}}}}) {
        aggregate {
        count
        }
    }
    }
    """
    #本周标注量（不去重，本周标注池完成的题目数量）
    query_of_current_week_annotated_data="""
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
    

    start_time =  "2021-10-08 8:00:00"
    end_time =  "2021-10-08 19:00:00"
    file = "/Users/lizhe/Desktop/shangqi-hasura.json"   # 存放url、pwd和token的json
    with open(file,'r') as f:
        obj = json.load(f)
        url = obj["url"]
        pwd = obj["x-hasura-admin-secret"]
        token = obj["token"]
    target_table_url = "https://api.notion.com/v1/databases/3d40984aec444edaa74d1d2dbc4402b8/query"
    to = "/Users/lizhe/Desktop/shangqi-{type}-{start}-{end}.csv"

    #统计本周标注
    projects_and_pool_ids = read_table(target_table_url,token,col_name="标注池ID")
    get_result(projects_and_pool_ids,
                query=query_of_current_week_annotated_data,
                payload_variables_structure=payload_variables_structure,
                to_file=to.format(type="current_week_annotated",start=start_time,end=end_time))
    
    #统计本周验收
    projects_and_pool_ids = read_table(target_table_url,token,col_name="客户抽检池ID")
    print(projects_and_pool_ids)
    get_result(projects_and_pool_ids,
                query=query_of_current_week_accepted_data,
                payload_variables_structure=payload_variables_structure,
                to_file=to.format(type="current_week_accepted",start=start_time,end=end_time))