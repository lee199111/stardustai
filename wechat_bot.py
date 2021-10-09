import requests
import shangqi_statistics as ss
webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ae54da2e-809f-47bf-90cc-d10c9a0a27da"
msg = """
{
    "msgtype": "text",
    "text": {
        "content": "hello"
    }
}
"""
r = requests.post(webhook,data=msg.encode('utf-8'))
print(r.text)