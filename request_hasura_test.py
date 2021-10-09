import requests
import json
url = "https://hasura.xcbiaozhu.saicsdv.com/v1/graphql"
headers = {
    "content-type":"application/json",
    "x-hasura-admin-secret":"a-secret-that-nobody-knows"
}
query = """
query countTaskRunsByTimeAndPoolId($start_time: timestamptz = "", $end_time: timestamptz = "", $pool_ids: [Int!] = 10) {
  task_runs_aggregate(where: {finished_at: {_gte: $start_time}, _and: {finished_at: {_lte: $end_time}, _and: {pool_id: {_in: $pool_ids}}}}) {
    aggregate {
      count
    }
  }
} 
"""

payload = {
    "query": query,
    "variables":
    {
        "start_time": "2020-9-18 8:00:00",
        "end_time": "2021-9-24 17:00:00",
        "pool_ids":
        [
            41930,
            35321,
            35325,
            35319,
            36849,
            36852,
            43317
        ]
    },
    "operationName": "countTaskRunsByTimeAndPoolId"
}

r = requests.post(url,headers=headers,data=json.dumps(payload))
print(r.json())