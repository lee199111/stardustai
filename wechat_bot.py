import requests

def send_notification(msg='',webhook=''):
    if webhook == '':
        webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ae54da2e-809f-47bf-90cc-d10c9a0a27da"
    if msg == '':
        msg = """
        {
            "msgtype": "text",
            "text": {
                "content": "hello"
            }
        }
        """ 
    r = requests.post(webhook,data=msg.encode('utf-8'))
    # print(r.text)

if __name__ == "__main__":
        #send_notification
    # webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=bc595d9b-3028-417e-bf43-2f2dc7e1b9e2"
    msg = '''{
  "msgtype": "text",
  "text": {
    "content": "Hi，@张文静，今天上汽的统计已发送的邮箱，请查收\n邮箱地址：https://mail.google.com/mail/u/0/#""
  }
}'''
    send_notification(msg=msg)  