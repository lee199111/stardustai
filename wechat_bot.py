import requests
webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=d6e82e4e-3dad-4b11-a799-9631885992fa"
msg = """
{
    "msgtype": "text",
    "text": {
        "content": "你啥玩意儿"
    }
}
"""
r = requests.post(webhook,data=msg.encode('utf-8'))
print(r.text)