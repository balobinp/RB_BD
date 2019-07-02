# Auto sender

import csv
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from os.path import join
import os

from_date_price = 'August 01, 2019' # The Date to apply the tariffs. Will be added to the letter body
downloads = r'c:\W_DATA_ROAM\Ã≈“Œƒ» »\Ã_PRICE_PLAN_UPDATE_PARTNER_P4_2019\Step_07_Send_PP_to_Customers\AUTO_MAIL_SENDER'
contacts_filename = "contacts_file.csv"
# Exzample:
# customer,name,email,prices
# Naka,Pavel,pavel@roamability.com;balobin.p@mail.ru,S1
# Telzar,Pavel,pavel@roamability.com,S1;S2
# STI,Pavel,pavel@roamability.com,S1;S2;Comb
s1_filename = "Roamability Roaming Rates 2019-06-01 Ver S1 1.1.1.xlsx"
s2_filename = "Roamability Roaming Rates 2019-04-10 Ver S2 1.1.1.xlsx"
comb_filename = "Roamability Roaming Rates 2019-06-01 Ver Combined 1.1.4.xlsx"

port = 465  # For SSL
smtp_server = "smtp.gmail.com"
# sender_email = "balobin.p@gmail.com"
# password = ""
sender_email = "pavel@roamability.com"
password = ""
cc_emails = ["balobin.p@mail.ru", "balobin.p@gmail.com"]
bcc_emails = ["balobin.p@gmail.com"]

contacts_filepath = join(downloads, contacts_filename)
s1_filepath = join(downloads, s1_filename)
s2_filepath = join(downloads, s2_filename)
comb_filepath = join(downloads, comb_filename)

# Create secure connection with server and send email
context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    with open(contacts_filepath) as file:
        reader = csv.reader(file, delimiter=",")
        next(reader)  # Skip header row
        for customer, name, email, prices in reader:
            mail_list = email.split(';') # Convert to list
            print(f"Sending mail to {customer} - {name}")
            print(f"    To: {email}")
            
            # "The msg['To'] needs to be a string:
            message = MIMEMultipart()
            message["Subject"] = f"Roamability Pricelist update from {from_date_price} for {customer}"
            message["From"] = sender_email
            message["To"] = ", ".join(mail_list) # Convert to string
            message["Cc"] = ", ".join(cc_emails) # Convert to string
            
            receiver_emails = []
            receiver_emails.extend(mail_list)
            receiver_emails.extend(cc_emails)
            receiver_emails.extend(bcc_emails)
                        
            html = """\
            <div>
            <table class=MsoNormalTable border=0 cellspacing=5 cellpadding=0
             width="100%" style='width:100.0%;mso-cellspacing:1.5pt;mso-yfti-tbllook:
             1184'>
             <tr style='mso-yfti-irow:0;mso-yfti-firstrow:yes'>
              <td width="100%" style='width:100.0%;padding:0cm 0cm 0cm 0cm'>
              <p class=MsoNormal align=center style='text-align:center'><span
              style='font-family:ArialMT;mso-fareast-font-family:"Times New Roman";
              color:#393A3D'><img height=135 id="_x0000_i1025"
              src="https://c40.qbo.intuit.com/qbo40/ext/Image/show/115384367057025/1?14746272490006"
              style='height:135px'><o:p></o:p></span></p>
              </td>
             </tr>
             <tr style='mso-yfti-irow:1;mso-yfti-lastrow:yes'>
              <td style='padding:10.5pt 0cm 0cm 0cm'>
              <p class=MsoNormal align=center style='text-align:center'><span
              style='font-size:15.0pt;font-family:ArialMT;mso-fareast-font-family:"Times New Roman";
              color:#0E909A'>Roamability, LLC<o:p></o:p></span></p>
              </td>
             </tr>
            </table>
            </div>
            <div style='margin-left:15.0pt' id=emailContainer>
                <p><span style='font-size:13.5pt;font-family:ArialMT;color:#393A3D'>Dear
                {name},<br><br>
                With this e-mail, we inform you of the changes in our price list.<br>
                Please, find the new price list in attach.<br><br>
                <span style='font-size:13.5pt;font-family:ArialMT;color:#393A3D'>
                The new Price takes effect from 
                <span style="text-decoration: underline; color: #ff0000;"><strong>{from_date_price}</strong></span>.<o:p></o:p></span><br><br>
                Note, no networks were added nor removed from Your allowed list of networks, if any changes in your allowed list are required, please submit a ticket to our support at support@roamability.com.<br><br>
                Any discounts and conditions that were agreed before, are applicable on these rates as well.
                <br><br>
                Please, feel free to contact our billing team at billing@roamability.com if you have any questions or require any further information.
                <br><br>
                Thanks for your business!<br>
                Roamability, LLC<o:p></o:p></span></p>

            </div>
            <div>
                <div style='margin-top:11.25pt'>
                <p class=MsoNormal align=center style='text-align:center'><span
                style='font-size:11.5pt;font-family:ArialMT;mso-fareast-font-family:"Times New Roman";
                color:#6B6C72'>848 North Rainbow Boulevard #546 Las Vegas, NV 89107 US<o:p></o:p></span></p>
                </div>
            </div>
            <div>    
            <span style='font-size:6pt;font-family:ArialMT;color:#393A3D'>
                <p>Best Regards,<br />
                Pavel Balobin<br />
                +79217428080<br />
                pavel@roamability.com<br />
                Skype: balobin.p</p>
            </span>
            </div>
            """
            
            message.attach(MIMEText(html.format(name=name, from_date_price=from_date_price), "html"))
            
            price_lists = []
            for price in list(prices.lower().split(';')):
                if price == 's1':
                    price_lists.append([s1_filename, s1_filepath])
                if price == 's2':
                    price_lists.append([s2_filename, s2_filepath])
                if price == 'comb':
                    price_lists.append([comb_filename, comb_filepath])

            for file, file_path in price_lists:
                print(f"    File: {file}")
                # Open file in binary mode
                with open(file_path, "rb") as attachment:
                    # Add file as application/octet-stream
                    # Email client can usually download this automatically as attachment
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
                # Encode file in ASCII characters to send by email    
                encoders.encode_base64(part)
                # Add header as key/value pair to attachment part
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={file}",)
                # Add attachment to message and convert message to string
                message.attach(part)

            server.sendmail(sender_email, receiver_emails, message.as_string())
# (Õ‡ ÓÒÌÓ‚Â: https://realpython.com/python-send-email/)
# (œÓ Bcc: https://stackoverflow.com/questions/1546367/python-how-to-send-mail-with-to-cc-and-bcc)
# "The msg['To'] needs to be a string:
# While the recipients in sendmail(sender, recipients, message) needs to be a list"
# (https://stackoverflow.com/questions/8856117/how-to-send-email-to-multiple-recipients-using-python-smtplib)