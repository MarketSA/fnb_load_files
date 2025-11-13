import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from pathlib import Path
from email.mime.application import MIMEApplication
from data import log

# receiver_email = "givenk@marketsa.co.za"
def sendEMail(receiver_email, msg, subject, attch_path = [], sender_email= "alerts@marketsa.co.za", password="QZ4HK2eR^awf`7%p)~xCc3"):
    res = False, "Nothing happened"
    print("Starting email process")
    message = MIMEMultipart()
    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = ','.join(receiver_email)

    email_body = MIMEText(msg, "html")
    message.attach(email_body)

    if len(attch_path) > 0:
        for path in attch_path:
            part = MIMEBase('application', "octet-stream")
            with open(path, 'rb') as file:
                part = MIMEApplication(file.read(), Name=Path(path).name)
            part.add_header('Content-Disposition', f'attachment; filename="{Path(path).name}"')
            message.attach(part)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP("za-smtp-outbound-1.mimecast.co.za", 587) as server:
            server.starttls(context=context)
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        res = True, f"email sent to {','.join(receiver_email)}"

    except Exception as e:
        res = False, f"Error in sening email{e}. On line {e.__traceback__.tb_lineno}"
        print(f'Error in sending email=> {e}',  '\non line+>', e.__traceback__.tb_lineno)
        log(f'Error in sending email=> {e}', f'\non line+> {e.__traceback__.tb_lineno}')
    
    return res
