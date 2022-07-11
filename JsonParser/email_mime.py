from email import encoders
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import boto3

def send_email_with_attachment(
        ses_client = None,
        email_src = '', 
        emails_dest:list = [''],
        subject_text:str = '',
        body_text:str = '',
        region_name:str = 'us-east-1', 
        file_path:str = '',
        charset:str = 'UTF-8'
    ):
    if ses_client is None:
        ses_client = boto3.client("ses", region_name = region_name)

    msg = MIMEMultipart()
    msg["Subject"] = subject_text
    msg["From"] = email_src
    
    # Set message body
    body = MIMEText(body_text, "plain")
    msg.attach(body)

    with open(file_path, "rb") as attachment:
        part = MIMEApplication(attachment.read())
        part.add_header("Content-Disposition",
                        "attachment",
                        filename=file_path)
    msg.attach(part)

    #for dest in emails_dest:
        #msg["To"] = dest
        # Convert message to string and send
    response = ses_client.send_raw_email(
        Source=email_src, 
        Destinations=emails_dest,
        RawMessage={"Data": msg.as_string()}
    )
    print(response)

def send_email(
    ses_client = None,
    email_src = '', 
    emails_dest:list = [''],
    subject_text:str = '',
    body_type:str = 'Text',# or 'Html'
    body_text:str = '',
    region_name:str = 'us-east-1', 
    charset:str = 'UTF-8'
    ):
    if ses_client is None: 
        ses_client = boto3.client("ses", region_name=region_name)

    response = ses_client.send_email(
        Destination={
            "ToAddresses":emails_dest,
        },
        Message={
            "Body": {
                body_type: {
                    "Charset": charset,
                    "Data": body_text,
                }
            },
            "Subject": {
                "Charset": charset,
                "Data": subject_text,
            },
        },
        Source=email_src,
    )
