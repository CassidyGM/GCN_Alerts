#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function
import numpy as np
import pandas as pd
import os.path
import datetime as dt
from gcn_kafka import Consumer
from confluent_kafka import TopicPartition
from datetime import date
from pathlib import Path
import voeventparse as vp
import astropy.units as u
from astropy.coordinates import AltAz, EarthLocation, SkyCoord, get_body, get_constellation
from astropy.time import Time
from astroplan import Observer, FixedTarget
from astroplan import (AltitudeConstraint, AirmassConstraint, AtNightConstraint)
from astroplan import is_observable


# Define constants and telescope parameters
q_vec = [0.05, 0.1, 0.075, 0.2, 0.1, 0.075, 0.09, 0.15, 0.6, 0.9, 0.92, 0.9, 0.7, 0.4, 0.15]
wav_vec = [200, 220, 230, 240, 260, 280, 300, 340, 410, 500, 580, 660, 800, 900, 1000]
phase_vec1 = [0, 90, 180, 270, 360]
phase_vec2 = [0, 180, 360]
r = 50.8/2 # in cm
rn = 8.7 # e-
T = 0.0021
D = 0.14 # e/pix/sec
gain = 1.2 # e-/ADU
lati = -42.43
long = 147.3
hght = 646 # m
# Define the observatory
bisdee_tier = EarthLocation(lat=lati*u.deg, lon=long*u.deg, height=hght*u.m)
GHO = Observer(location=bisdee_tier, name="Greenhill Observatory", timezone="Australia/Tasmania")


# Connect as a consumer.
# Warning: don't share the client secret with others.
consumer = Consumer(client_id='3a55lngrsah42c46ofhntenumf',
                    client_secret='179chfu1lbg08flmq6nbiongbpaodvn14h39his89ob77jvn970t',
                    domain='gcn.nasa.gov')

## Timestamps between which you want to search for alerts
timestamp1 = int((dt.datetime.now() - dt.timedelta(days=7)).timestamp() * 1000)
# days=? is how many days in the past to start the search
timestamp2 = timestamp1 + 7*86400000
# 86400000 = +1 day from start date

## Set date range for astro plan to check observability
## Set to check the next 3 days from 'today'
date1=date.today()
date2=date1+dt.timedelta(days=3)
time_range = Time([str(date1), str(date2)])
## Constrain to observable angles and during local night time
constraints = [AltitudeConstraint(10*u.deg, 80*u.deg),
               AirmassConstraint(5), AtNightConstraint.twilight_civil()]


## List the GCN alert streams to be searched
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

## Create lists of the information desired from the alerts
who_list=[]
id_list=[]
date_list=[]
ra_list=[]
dec_list=[]
error_list=[]
notice_list=[]
file_list=[]

##Make a directory of todays date to save alerts into
##Change name from today if looking for alerts on other dates
#today = str(date.today())
#path='./Obs_Events_'+today
#if not os.path.exists(path):
#    os.mkdir(path)
topictest=['gcn.classic.text.FERMI_GBM_ALERT',
             'gcn.classic.text.FERMI_GBM_FIN_POS',
            'gcn.classic.text.FERMI_GBM_FLT_POS',
            'gcn.classic.voevent.INTEGRAL_SPIACS']

##Check new alerts
## Start counter for how many alerts are found
n=0
for a in topictest:
    topic = a
    #print(topic)

    ## Sets the dates to search over from above
    start = consumer.offsets_for_times(
        [TopicPartition(topic, 0, timestamp1)])
    end = consumer.offsets_for_times(
        [TopicPartition(topic, 0, timestamp2)])

    try:
        consumer.assign(start)
        for message in consumer.consume(end[0].offset - start[0].offset, timeout=1):
            # Prints all alert messgaes found   
            print(message.value())     
            n=n+1
            ## Save the VOEvent files into the 'today' directory
            # Name file Alert with a number - VOEvent file
            datafilename = str(n)+str(topic)+'.xml'
            # Save into the new folder for the day
            locname = os.path.join(path,datafilename)
            with open(locname, 'wb') as f:
                np.save(f, message.value())
            
            ## Open the file to search for information
            with open(locname, 'rb') as f:
                #print(f)
                next(f)
                next(f)
                v = vp.load(f)

                ## Get RA/Dec of event (central)
                #print(v.Who.Author.shortName.text)
                RA=str(v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Value2.C1)
                print('Event Ra: '+ RA)
                Dec=str(v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Value2.C2)
                print('Event Dec: '+ Dec)

                ## Create coordinate in degrees
                c = SkyCoord(RA, Dec, unit="deg")

                ## Check if event coord is observable at all (any time of night) from out telescope
                ever_observable = is_observable(constraints, GHO, c, time_range=time_range)
                
                ## If event is observable:
                if ever_observable[0] == True:
                    ## Print event information
                    #print(str(v.Who.Author.shortName.text)+' event ' +str(datafilename) +' is observable. RA:' +str(RA)+' Dec: '+str(Dec))
                    
                    ## Save relevant information into the lists
                    who_list.append(v.Who.Author.shortName.text)
                    date_list.append(v.Who.Date)
                    ra_list.append(RA)
                    dec_list.append(Dec)
                    error_list.append(v.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Error2Radius)
                    ##Get observatory event ID number
                    id=v.find(".//Param[@name='Trans_Num']").attrib['value']
                    id_list.append(id)
                    #notice_list.append(v.)
                    #file_list.append(datafilename)

            ## Delete the VOEvent files
            os.remove(datafilename)

        ## Name file Alert with a number - text file
        #datafilename = str(n)+str(topic)+'.txt'
        ## Save into the new folder for the day
        #locname = os.path.join(path,datafilename)
        #with open(locname, 'wb') as f:
        #    np.save(f, message.value())

    ## Except to ignore error that occurs when there are no new alerts
    except ValueError: 
        pass
        
    consumer.pause(start)

## Save information in lists into pandas data frame
d = {'Who':who_list,'Detection Date':date_list,'ID':id_list,
     'RA':ra_list,'Dec':dec_list,'Error Radius':error_list,'File name':file_list}
df = pd.DataFrame(data=d)
if df.empty == True:
    df['Who']=['No Alerts']

# If no alerts found in this time period, print 'No new alerts'
#if n==0:
#    print('No new alerts')
#    df = pd.DataFrame({"Events": ['No Alerts']})
# Else print the number of alerts found
#else:
#    print('Done: '+str(n)+' events')


## Save data frame to csv file
today = str(date.today())
df.to_csv('Obs_events_'+today+'.txt', sep='\t', index=False)

consumer.close()



