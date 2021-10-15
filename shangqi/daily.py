from typing import Type
import requests
import json
import re
import pandas as pd
from send_email import send_email
from wechat_bot import send_notification
from shangqi_statistics import confirm_choice,col_type,read_table,get_result_from_hasura,set_variables,write_csv,auth,run
import os



#统计某个时间段内某个工作池创建量（不去重）
query_of_created_count = """
query query_of_created_count($start_time: timestamptz = "", $end_time: timestamptz = "", $pool_ids: [Int!] = 10) {
task_runs_aggregate(where: {created_at: {_gte: $start_time}, _and: {created_at: {_lte: $end_time}, _and: {pool_id: {_in: $pool_ids}}}}) {
    aggregate {
    count
    }
}
}
"""
#统计某个时间段内的某个工作池创建量（去重）
query_of_distinct_created_count = '''
query query_of_distinct_created_count($start_time: timestamptz = "", $end_time: timestamptz = "", $pool_ids: [Int!] = 10){
  tasks_aggregate(where:{task_runs:{created_at: {_gte: $start_time}, _and: {created_at: {_lte: $end_time}, _and: {pool_id: {_in: $pool_ids}}}}}){
    aggregate{
      count
    }
  }
}
'''

#统计某个时间段内的某个工作池完成量（不去重）
query_of_finished_count="""
query query_of_finished_count($start_time: timestamptz = "", $end_time: timestamptz = "", $pool_ids: [Int!] = 10) {
task_runs_aggregate(where: {finished_at: {_gte: $start_time}, _and: {finished_at: {_lte: $end_time}, _and: {pool_id: {_in: $pool_ids} _and:{task_run_status:{_eq:FINISHED}}}}}) {
    aggregate {
    count
    }
}
}
"""

# 统计某个时间段内的某个工作池完成量并且方向为forward（去重、总量）
query_of_finished_and_forward_count_all='''
query query_of_finished_and_forward_count_all($start_time: timestamptz = "", $end_time: timestamptz = "", $pool_ids: [Int!] = 10){
  tasks_aggregate(where:{task_runs:{finished_at: {_gte: $start_time}, _and: {finished_at: {_lte: $end_time}, _and: {pool_id: {_in: $pool_ids} _and: {task_run_submit_direction: {_eq: FORWARD} }} }}}){
    aggregate{
      count
    }
  }
}'''

# 统计某个时间段内的某个工作池完成量并且方向为forward（去重、还在工作池）
query_of_finished_and_forward_and_not_moved_count='''
query query_of_finished_and_forward_and_not_moved_count($start_time: timestamptz = "", $end_time: timestamptz = "", $pool_ids: [Int!] = 10){
  tasks_aggregate(where:{task_runs:{finished_at: {_gte: $start_time}, _and: {finished_at: {_lte: $end_time}, _and: {pool_id: {_in: $pool_ids} _and: {task_run_submit_direction: {_eq: FORWARD} _and:{task_run_status:{_neq:FINISHED}}}} }}}){
    aggregate{
      count
    }
  }
}'''

# 统计某个时间段内的某个工作池完成量并且方向为forward（去重、不在工作池）
query_of_finished_and_forward_and_moved_count='''
query query_of_finished_and_forward_and_moved_count($start_time: timestamptz = "", $end_time: timestamptz = "", $pool_ids: [Int!] = 10){
  tasks_aggregate(where:{task_runs:{finished_at: {_gte: $start_time}, _and: {finished_at: {_lte: $end_time}, _and: {pool_id: {_in: $pool_ids} _and: {task_run_submit_direction: {_eq: FORWARD} _and:{task_run_status:{_eq:FINISHED}}}} }}}){
    aggregate{
      count
    }
  }
}'''



s_e_p_variables =     {
        "start_time": "2020-9-18 8:00:00",
        "end_time": "2021-9-24 17:00:00",
        "pool_ids":
        [
            41930,
            35321
        ]
    }


start_time =  "2021-10-12 20:00:00"
end_time =  "2021-10-13 20:00:00"
confirm_msg = "起始时间为：{}       截止时间为:{}\n".format(start_time,end_time)
# confirm_choice(confirm_msg) #confirm
auth_file = "/Users/lizhe/Desktop/shangqi-hasura.json"   # 存放 url、pwd 和 token 的 json
target_table_url = "https://api.notion.com/v1/databases/3d40984aec444edaa74d1d2dbc4402b8/query"
to = "shangqi_{}_{}.xls".format(start_time,end_time)

sheets = {"星尘提交量（不去重）":["客户抽检池",query_of_created_count],
          "星尘提交量（去重）":["客户抽检池",query_of_distinct_created_count],
          "上汽验收合格总量":["客户抽检池",query_of_finished_and_forward_count_all],
          "上汽验收合格量（还在上汽抽检池）":["客户抽检池",query_of_finished_and_forward_and_not_moved_count],
          "上汽验收合格量（已出上汽抽检池）":["客户抽检池",query_of_finished_and_forward_and_moved_count]
          }

try:
  os.remove(to)
except:
  pass

for k,v in sheets.items():
  run(auth_file=auth_file,
      table_url=target_table_url,
      col_name=v[0],
      start=start_time, 
      end=end_time,
      hasura_query=v[1],
      hasura_variables=s_e_p_variables,
      to_file=to,
      to_sheet=k)
