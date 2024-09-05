"""
GCN_alerts_mail.py

Author: Cassidy Mihalenko & Tom Plunkett 

Date: 17/03/24
Updated: 12/08/2024

Purpose: Download all new GCN alerts from chosen streams and check of observable events,
then send a collated .csv file to potential observers via email.
"""
# Import necessary packages 
import numpy as np
import pandas as pd
import os
import datetime as dt
from datetime import datetime 
from datetime import date
import mimetypes
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import smtplib, ssl
from gcn_kafka import Consumer
from confluent_kafka import TopicPartition
from pathlib import Path
import voeventparse as vp
import astropy.units as u
from astropy.coordinates import AltAz, EarthLocation, SkyCoord, get_body, get_constellation
from astropy.time import Time
from astroplan import Observer, FixedTarget
from astroplan import (AltitudeConstraint, AirmassConstraint, AtNightConstraint)
from astroplan import is_observable

# Define home folder
main_folder = os.path.abspath('/home/obs/GCN_Alerts/')

# Get dates
date_str = str(datetime.now().date())
year_str = str(datetime.now().year)

# Define constants and location
lati = -42.43
long = 147.3
hght = 646 # m
# Define the observatory
bisdee_tier = EarthLocation(lat=lati*u.deg, lon=long*u.deg, height=hght*u.m)
GHO = Observer(location=bisdee_tier, name="Greenhill Observatory", timezone="Australia/Tasmania")

def get_alerts():
    # Connect as a consumer.
    # Warning: don't share the client secret with others.
    consumer = Consumer(client_id='7agc4r0vqmkd6m6ur4bisgk6a',
                    client_secret='12d773gsp10de2ij7sidm91gvdq1mh8grja6a1i90fiqanssf7nv',
                    domain='gcn.nasa.gov')


    ## Timestamps between which you want to search for alerts
    ## days=? is the numbers of days ago you want to start checking for alerts
    timestamp1 = int((datetime.now() - dt.timedelta(days=1)).timestamp() * 1000)
    timestamp2 = timestamp1 + 86400000 # +1 day from start date

    date1=dt.date.today()
    date2=date1+dt.timedelta(days=4)
    time_range = Time([str(date1), str(date2)])

    constraints = [AltitudeConstraint(10*u.deg, 80*u.deg),
               AirmassConstraint(3), AtNightConstraint.twilight_nautical()]


    topiclist = ['gcn.classic.voevent.FERMI_GBM_SUBTHRESH',
                'gcn.classic.voevent.FERMI_GBM_FIN_POS',
                #’gcn.classic.voevent.MAXI_UNKNOWN',
                'gcn.classic.voevent.SWIFT_XRT_POSITION',
                'gcn.classic.voevent.FERMI_GBM_FLT_POS',
                'gcn.classic.voevent.GRB_CNTRPART',
                'gcn.classic.voevent.FERMI_LAT_OFFLINE',
                #'gcn.classic.voevent.LVC_RETRACTION',
                'gcn.classic.voevent.GECAM_GND',
                'gcn.classic.voevent.SWIFT_BAT_GRB_POS_ACK',
                #'gcn.classic.voevent.LVC_INITIAL',
                #'gcn.classic.voevent.SWIFT_XRT_LC',
                'gcn.classic.voevent.SWIFT_UVOT_POS',
                'gcn.classic.voevent.INTEGRAL_OFFLINE',
                #’gcn.classic.voevent.SWIFT_XRT_SPECTRUM_PROC',
                'gcn.classic.voevent.FERMI_GBM_GND_INTERNAL',
                'gcn.classic.voevent.IPN_RAW',
                'gcn.classic.voevent.SWIFT_BAT_GRB_LC',
                'gcn.classic.voevent.LVC_COUNTERPART',
                #’gcn.classic.voevent.SWIFT_XRT_SPECTRUM',
                #'gcn.classic.voevent.LVC_UPDATE',
                #’gcn.classic.voevent.LVC_PRELIMINARY',
                #'gcn.classic.voevent.FERMI_GBM_FLT_INTERNAL',
                'gcn.classic.voevent.COINCIDENCE',
                #'gcn.notices.swift.bat.guano',
                'gcn.classic.voevent.FERMI_GBM_GND_POS',
                'gcn.classic.voevent.INTEGRAL_REFINED',
                #’gcn.classic.voevent.KONUS_LC’,
                'gcn.classic.voevent.HAWC_BURST_MONITOR',
                'gcn.classic.voevent.GECAM_FLT'
                #’gcn.classic.voevent.CALET_GBM_FLT_LC',
                #’gcn.classic.voevent.AMON_NU_EM_COINC'
                ]
    ## https://gcn.gsfc.nasa.gov/archives.html

    who_list=[]
    date_list=[]
    ra_list=[]
    dec_list=[]
    error_list=[]
    stream_list=[] 

    n=0
    for a in topiclist:
        topic = a
        start = consumer.offsets_for_times(
            [TopicPartition(topic, 0, timestamp1)])
        end = consumer.offsets_for_times(
            [TopicPartition(topic, 0, timestamp2)])
    
        try:
            consumer.assign(start)
            for message in consumer.consume(end[0].offset - start[0].offset, timeout=1):
                n=n+1
                # Name file Alert with a number - VOEvent file
                datafilename = str(n)+str(topic)+'.xml'
                # Save into the new folder for the day
                locname = os.path.join(path,datafilename)
                with open(locname, 'wb') as f:
                    np.save(f, message.value())
            
                with open(locname, 'rb') as f:
                    next(f)
                    next(f)
                    v = vp.load(f)
                    RA=str(v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Value2.C1)
                    Dec=str(v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Value2.C2)
                    c = SkyCoord(RA, Dec, unit="deg")
                    ever_observable = is_observable(constraints, GHO, c, time_range=time_range)
                
                    if ever_observable[0] == True:
                        who_list.append(v.Who.Author.shortName.text)
                        date_list.append(v.Who.Date)
                        ra_list.append(RA)
                        dec_list.append(Dec)
                        error_list.append(v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Error2Radius)
                        stream_list.append(str(topic))
                        
                os.remove(datafilename)


        except ValueError: 
            pass
        
        consumer.pause(start)


        d = {'Who':who_list,'Detection Date':date_list,'Stream':stream_list,#'ID':id_list,
            'RA':ra_list,'Dec':dec_list,'Error Radius':error_list}#,'File name':file_list}
        df = pd.DataFrame(data=d)
        if df.empty == True:
            df['Who']=['No Alerts']
            
        today = str(date.today())
        df.to_csv('Obs_events_'+date_str+'.txt', sep='\t', index=False)

    consumer.close()



def send_gcn_mail():
    """
    Send the email containing the list of interesting microlensing events for the next few weeks.
    """
    
    MESSAGE_BODY = "Hello! \n\nCassidy's GCN alerts script has found observable GRBs for the H50 telescope. Please find attached the list of targets in .CSV format. For more information, ask Cassidy. \n\n Clear skies! \n\n"

    # Create a multipart message
    msg = MIMEMultipart()
    body_part = MIMEText(MESSAGE_BODY, 'plain')
    msg['Subject'] = "GRB Schedule for UTGO - " + str(datetime.now().date())
    msg['From'] = 'GreenhillObservatory@gmail.com'
    recipients = ['cassidy.mihalenko@utas.edu.au', 'karelle.siellez@utas.edu.au']
    msg['To'] = ", ".join(recipients)
    
    # Add body to email
    msg.attach(body_part)
    
    # List the csv files for sending
    csv_files = [str('Obs_events_'+date_str+'.txt')]
    
    # Bit of magic to get shit to work
    ctype, encoding = mimetypes.guess_type(csv_files[0])
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"
    maintype, subtype = ctype.split("/", 1)
    
    # Loop of the files to send and attach them to the email
    for file in csv_files:
        with open(file) as fp:
            attachment = MIMEText(fp.read(), _subtype=subtype)
            attachment.add_header("Content-Disposition", "attachment", filename=file)
            msg.attach(attachment)
            fp.close()
    
    # Open a gmail server and send the email
    server_ssl = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server_ssl.ehlo() # optional, called by login()
    server_ssl.login('greenhillobservatory@gmail.com', 'twza hxvz sdae rinh')
    server_ssl.sendmail(msg['From'], recipients, msg.as_string())
    server_ssl.close()

if __name__ == '__main__':
    os.chdir(main_folder)
    get_alerts()
    send_gcn_mail()

    
    
    
    
    
    
