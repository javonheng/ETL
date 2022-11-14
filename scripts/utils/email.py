import smtplib, ssl
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from .log import log
from decouple import config
import os

def send_email(logfile_name, subject, body, attachment_files):

    current_directory = os.getcwd()
    print(current_directory)
    gmail_user = config('EMAIL_USER')
    gmail_password = config('EMAIL_PASSWORD')

    sent_from = gmail_user
    to = ['jjavonhengg@gmail.com', 'javonheng@hotmail.com']
    subject1 = 'ETL Completed - ' + subject

    email_text = """\
    <div>From: %s <br>
    To: %s <br>
    Subject: %s
    </div>

    %s
    """ % (sent_from, COMMASPACE.join(to), subject1, body)

    # Set message content
    msg = MIMEMultipart('mixed')
    # msg.set_content(email_text)
    msg['Subject'] = subject1
    msg['From'] = sent_from
    msg['Date'] = formatdate(localtime=True)
    msg['To'] = COMMASPACE.join(to)
    msg.attach(MIMEText(email_text, 'html'))

    for f in attachment_files or []:
      path = current_directory+f
      print(path)
      if os.path.exists(path):
        try:
          with open(path, "rb") as fil:
            part = MIMEApplication(
              fil.read(),
              Name=basename(f)
            )
          # After the file is closed
          part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
          msg.attach(part)
        except Exception as e:
          print(str(e))

    try:
      with smtplib.SMTP("smtp.gmail.com", port=587) as smtp:
        smtp.starttls(context=ssl.create_default_context())
        smtp.login(gmail_user, gmail_password)
        smtp.sendmail(sent_from, to, msg.as_string())
        smtp.quit()

        log(logfile_name, "INFO", 'Email sent successfully!')
    except Exception as ex:
      log(logfile_name, "ERROR", 'Email not sent: ' + str(ex))


def draft_email_body(inserted, err_inserted, time_taken):
    return '<div>\
    Your ETL Pipeline has been completed. <br><br>  Number of Rows Inserted: <strong>%s</strong> <br>  Number of Errors: <strong>%s</strong> <br>  Time taken for insertion: <strong style="color:red;">%s</strong> ms<br><br>  Thank you. \
    </div>\
    ' % (inserted, err_inserted, time_taken)
