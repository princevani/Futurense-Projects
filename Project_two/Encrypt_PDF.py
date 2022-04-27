# Project 2

# Required Libraries
import hashlib
import maskpass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import os
import PyPDF2 as p
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

# Defining the variables and path of the required files
meta = MetaData()
file = os.listdir(r"C:\Users\princ\Desktop\pdf")

# DEFINE THE DATABASE CREDENTIALS
user = 'root'
password = '9662'
host = '127.0.0.1'
port = 3306
database = 'pdf_data'
tablename = "students"

# pdf passwords > contain encrypted passwords of each pdf:
passwords = {}


# Generating encrypted pass for pdf

def pdf_pass(arg):
    hash_func = hashlib.sha1()
    string = f"{arg}"
    encoded_string = string.encode()
    hash_func.update(encoded_string)
    password = hash_func.hexdigest()
    return password


# Mailing the encrypted files and pass to the given email address
def mail_files(arg):
    mail_content = f'''Hello,
    This is a test mail.
    In this mail we are sending some attachments and passwords.
    The mail is sent using Python SMTP library. 

    {arg} 

    Thank You
    '''
    # The mail addresses and password
    sender_address = 'shreyagupt79@gmail.com'
    sender_pass = maskpass.askpass(mask="")
    receiver_address = 'princevani79@gmail.com'
    # Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Pdf files has been encypted successfully.'
    # The subject line
    # The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    attach_file_name = r"C:\Users\princ\Desktop\pdf_new\encr_sample.pdf"
    attach_file = open(attach_file_name, 'rb')  # Open the file as binary mode
    payload = MIMEBase('application', 'octate-stream')
    payload.set_payload((attach_file).read())
    encoders.encode_base64(payload)  # encode the attachment
    # add payload header with filename
    payload.add_header('Content-Decomposition', 'attachment', filename=attach_file_name)
    message.attach(payload)
    # Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587)  # use gmail with port
    session.starttls()  # enable security
    session.login(sender_address, sender_pass)  # login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()
    print('Mail Sent')


# saving the records of files and pass to the database using SQLAlchemy
engine = create_engine(url=f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
if sqlalchemy.inspect(engine).has_table(tablename):
    pass
else:
    students = Table('students', meta,
                     Column('id', Integer),
                     Column('pdf_name', String(40)),
                     Column('password', String(100)),
                     )
    meta.create_all(engine)

# Iterating to each pdf files
for j, i in enumerate(file):
    if i.endswith('.pdf'):
        output = p.PdfFileWriter()
        input_stream = p.PdfFileReader(open(rf"C:\Users\princ\Desktop\pdf\{i}", "rb"))

        for k in range(0, input_stream.getNumPages()):
            output.addPage(input_stream.getPage(k))

        outputstream = open(fr"C:\Users\princ\Desktop\pdf_new\encr_{i}", "wb")

        output.encrypt(i, use_128bit=True)
        output.write(outputstream)
        passwords[str(i)] = pdf_pass(i)
        engine.execute(F"INSERT INTO students VALUES ('{j}', 'encrypted_{i}', '{str(pdf_pass(i))}')")
        outputstream.close()

# Calling the mail function
mail_files(passwords)
