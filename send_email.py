import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from ssl import SSLContext
import glob

def send_email(files,sender,receiver,text=''):
    message = MIMEMultipart()
    message["From"] = sender
    message['To'] = receiver
    message['Subject'] = "sending mail using python"
    message.attach(MIMEText(text))

    # 想加几个文件就加几个文件
    for file in files:
        attachment = open(file,'rb')
        obj = MIMEBase('application','octet-stream')
        obj.set_payload((attachment).read())
        encoders.encode_base64(obj)
        obj.add_header('Content-Disposition',"attachment; filename= "+file)
        message.attach(obj)

    my_message = message.as_string()
    email_session = smtplib.SMTP('smtp.gmail.com',587)
    email_session.starttls()
    email_session.login(sender,'edkehefusumpmjab')
    email_session.sendmail(sender,receiver,my_message)
    email_session.quit()
    print("YOUR MAIL HAS BEEN SENT SUCCESSFULLY")


if __name__ == "__main__":
    files = ["/Users/lizhe/Desktop/stardustai/text.xls"]
    sender_email = "zhe.li@stardust.ai"
    receiver_email = "zhe.li@stardust.ai"
    send_email(files,sender_email,receiver_email)