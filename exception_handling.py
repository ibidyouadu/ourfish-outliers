import smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import tkinter as tk
import tkinter.messagebox as messagebox
import configparser

def send_error_log(logpath):
    """
    Send an email to Angel with Python error log.
    """
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    bot_email = cfg.get('email', 'bot_email')
    password = cfg.get('email', 'bot_password')
    adu_email = cfg.get('email', 'adu_email')

    port = 465 #for conntecting to gmail server
    # create secure SSL context ie security configuration
    context = ssl.create_default_context()

    # components of the email message to put into MIMEMultipart object
    from_address = bot_email
    to_address = adu_email
    subject = "Outlier Code Error"
    body = """
    AAAarrRrGGgghH! *cough* I;m dying *cooough* soomething went wrong...
    The error log, please... look for answers,.. in..side........

    Have a nice day!
    Outlier Bot (RIP)
    """

    # build the MIMEMultipart object which will later be converted to text
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject

    # add the message body
    msg.attach(MIMEText(body, "plain"))

    # attach csv file
    fname = str(logpath.name)
    with open(logpath, 'rb') as f:
        msg.attach(MIMEApplication(f.read(), Name=fname))

    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login(from_address, password)
        server.sendmail(from_address, to_address, msg.as_string())

def print_error_message(reason=None):
    root = tk.Tk()
    root.withdraw()
    if reason is None:
        window_title = "Outlier Detection Error"
        message = """Something went wrong with the outlier detection code.
        An error log has been sent to Angel. He will get back to you soon."""
    else:
        window_title = "Postgres Login Error"
        message = """Could not ping the database. Maybe login info is wrong?
        Please enter your login details again."""
    messagebox.showwarning(window_title, message)
    root.destroy()
