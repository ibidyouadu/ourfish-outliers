import smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import tkinter as tk
import tkinter.simpledialog as simpledialog
import configparser

def ask_email(window_title, prompt):
    """
    Helper function for prompting user email address
    """
    root = tk.Tk()
    root.withdraw()
    inp = simpledialog.askstring(window_title, prompt, parent=root)
    root.destroy()
    
    return inp

def ping(subject, body):
    
    # get email login info from config.ini
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    bot_email = cfg.get('email', 'address')
    password = cfg.get('email', 'password')
    
    port = 465
    context = ssl.create_default_context()
    from_address = bot_email
    to_address = bot_email
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject
    msg.attach(MIMEText(body, "plain"))
    
    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login(from_address, password)
        server.sendmail(from_address, to_address, msg.as_string())
    
def email_results(email, num_flagged, flagged_path, flagged_fname, images):
    """ 
    Send an email with the following information:

        1. All the information from the flagged records (so the whole rows)
        2. unit_price vs weight/count log-plots in the form of attachments
    
    Parameters
    ----------
    email (str)
        address that info will be sent to
    num_flagged: int
        number of records flagged as potential outliers
    flagged_path: str
        file path for the csv file containing flagged samples
    flagged_fname: tr
        file name for the csv
    images: dict[str]
        dictionary containing file paths for the images of the plots
    """

    port = 465 #for conntecting to gmail server
    context = ssl.create_default_context()

    # get email login info from config.ini
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    bot_email = cfg.get('email', 'address')
    password = cfg.get('email', 'password')

    # components of the email message to put into MIMEMultipart object
    from_address = bot_email
    to_address = email
    bcc_address = bot_email
    subject = "Potential Outliers Notice"
    body = """
    Good day! We counted %d record(s) from yesterday flagged as potential outlier(s).
    Please take a look at the csv/plot data, attached. As a reminder, the plot data is in a shifted log scale, so -1 on the graph means that the value is actually 0.
    
    Have a nice day!
    Outlier Bot
    """ % num_flagged
    receivers = [to_address, bcc_address]

    # build the MIMEMultipart object which will later be converted to text
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject
    msg['Bcc'] = bcc_address

    # add the message body
    msg.attach(MIMEText(body, "plain"))

    # attach csv file
    with open(flagged_path, 'rb') as f:
        msg.attach(MIMEApplication(f.read(), Name=flagged_fname))

    # attach images
    for n in range(0,len(images)):
        img_fname = images[n].name
        img_path = str(images[n])
        x_attach_id = str(n)
        content_id = '<' + str(n) + '>'

        with open(img_path, 'rb') as f:
            # initiate MIME object for img attachment
            mime = MIMEBase('image', 'png', filename=img_fname)
    
            # add metadata
            mime.add_header('Content-disposition', 'attachment', filename=img_fname)
            mime.add_header('X-attachment-id', x_attach_id)
            mime.add_header('Content-ID', content_id)
    
            # prepare the object in format compatible w msg, then attach to msg
            mime.set_payload(f.read())
            encoders.encode_base64(mime)
            msg.attach(mime)

    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login(from_address, password)
        server.sendmail(from_address, receivers, msg.as_string())