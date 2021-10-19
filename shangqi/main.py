import streamlit as st
import numpy as np
import pandas as pd
from shangqi_statistics import auth,read_table,get_result_from_hasura,write_csv,run_np
import os
import datetime


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


start_time =  "2021-10-8 20:00:00"
end_time =  "2021-10-15 20:00:00"
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

@st.cache
def convert_df(df):
    return df.to_csv().encode('utf-8')


def run(start="2021-10-8 20:00:00",end="2021-10-15 20:00:00"):
    result = {}
    hasura_queries = []
    for k,v in sheets.items():
        hasura_queries.append(v[1])
    with st.spinner('wait for it ...'):
        r = run_np(auth_file=auth_file,
                        table_url=target_table_url,
                        col_name=v[0],
                        start=start, 
                        end=end,
                        hasura_queries=hasura_queries,
                        hasura_variables=s_e_p_variables)
    st.success('Done!')
    return r
    

st.title('上汽统计')
today = str(datetime.date.today())
yestoday = str(datetime.date.today() - datetime.timedelta(1))
t1 = st.text_input('start',value=yestoday+' 20:00:00')
t2 = st.text_input('end',value=today+' 20:00:00')
button_click = st.button("确认",)
if button_click == True:
    r = run(t1,t2)
    columns = ["项目名称","项目类型"]+[k for k in sheets.keys()] + ["帧数--"+k for k in sheets.keys()]
    # print(columns)
    # write_csv(to,sheet_name="sheet_name",data=pd.DataFrame(r),header=columns)
    df = pd.DataFrame(r,columns=columns)
    csv = convert_df(df)
    st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='shangqi_{}_{}.csv'.format(t1,t2),
            mime='text/csv',
            )
    st.write(df)

print("waiting....🔞🔞🔞🔞🔞")








# # Create a text element and let the reader know the data is loading.
# data_load_state = st.text('Loading data...')

# # Load 10,000 rows of data into the dataframe.
# # Notify the reader that the data was successfully loaded.
# data_load_state.text('Loading data...done!')
