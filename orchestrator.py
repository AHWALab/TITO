"""
West Africa real-time model/subdomain execution script
Contributors:
Vanessa Robledo - vrobledodelgado@uiowa.edu
Humberto Vergara - humberto-vergaraarrieta@uiowa.edu
V.1.0 - February 01, 2024

This script consolidates execution routines in a single script, while
ingesting a "configuration file" from where a given model can be specified for a
given domain. Please use this script and a configuration file as follows:

    $> python final_name.py <configuration_file.py>

"""

from shutil import rmtree, copy
import os
from os import makedirs, listdir, rename, remove
import glob
from datetime import datetime as dt
from datetime import timedelta
import errno
import datetime
import time
import numpy as np
import re
import subprocess
import threading
import sys
import socket
import shutil
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from multiprocessing.pool import ThreadPool
import requests
from bs4 import BeautifulSoup
import datetime as DT
import osgeo.gdal as gdal
from osgeo.gdal import gdalconst
from osgeo.gdalconst import GA_ReadOnly
import time

from servir.scripts.m_nowcasting import load_default_params_for_model, nowcast
from servir.utils.m_h5py2tif import h5py2tif
from servir.utils.m_tif2h5py import tif2h5py

"""
Setup Environment Variables for Linux Shared Libraries and OpenMP Threads
"""

# Domain coordinates (This part must be changed)
xmin = -21.4
xmax = 30.4
ymin = -2.9
ymax = 33.1


def main(args):
    """Main function of the script.

    This function reads the real-time configuration script, makes sure the necessary files to run
    FLASH exist and are in the right place, runs the model(s), writes the outputs and states, and
    reports vie email if an error occurs during execution.

    Arguments:
        args {list} -- the first argument ([1]) corresponds to a real-time configuration file.
    """
        
    # Read the configuration file
    #config_file = __import__(args[1].replace('.py', ''))
    import westafrica1km_config as config_file
    domain = config_file.domain
    subdomain = config_file.subdomain
    systemModel = config_file.systemModel
    systemName = config_file.systemName
    ef5Path = config_file.ef5Path
    precipFolder = config_file.precipFolder
    statesPath = config_file.statesPath
    precipEF5Folder = config_file.precipEF5Folder
    modelStates = config_file.modelStates
    templatePath = config_file.templatePath
    template = config_file.templates
    nowcast_model_name = config_file.nowcast_model_name
    #DATA_ASSIMILATION = config_file.DATA_ASSIMILATION
    #assimilationPath = config_file.assimilationPath
    #assimilationLogs = config_file.assimilationLogs
    dataPath = config_file.dataPath
    qpf_store_path = config_file.qpf_store_path
    tmpOutput = config_file.tmpOutput
    SEND_ALERTS = config_file.SEND_ALERTS
    smtp_server = config_file.smtp_server
    smtp_port = config_file.smtp_port
    account_address = config_file.account_address
    account_password = config_file.account_password
    alert_sender = config_file.alert_sender
    alert_recipients = config_file.alert_recipients
    #MODEL_RES = config_file.model_resolution
    # SampleTIFF = config_file.sample_geotiff
    # product_Path = config_file.product_Path
    #geoFile = "/home/ec2-user/Scripts/post_processing/georef_file.txt"
    # thread_th = config_file.thread_th
    # distance_th = config_file.distance_th
    # Npixels_th = config_file.Npixels_th
    #copyToWeb = config_file.copyToWeb
    HindCastMode = config_file.HindCastMode
    HindCastDate = config_file.HindCastDate
    email = config_file.email
    server = config_file.server  
    
    # Real-time mode or Hindcast mode
    # Figure out the timing for running the current timestep
    if HindCastMode == True:
        currentTime = dt.strptime(HindCastDate, "%Y-%m-%d %H:%M") 
    else:
        currentTime = dt.utcnow()

    # Round down the current minutess to the nearest 30min increment in the past
    min30 = int(np.floor(currentTime.minute / 30.0) * 30)
    min60 = 0
    # Use the rounded down minutes as the timestamp for the current time step
    currentTime = currentTime.replace(minute=min60, second=0, microsecond=0)
    
    if HindCastMode == True:
        print("*** Starting hindcast run cycle at " + currentTime.strftime("%Y-%m-%d_%H:%M") + " UTC ***")
        print(" ") 
        print(" ") 
    else:
        print("*** Starting real-time run cycle at " + currentTime.strftime("%Y-%m-%d_%H:%M") + " UTC ***")
        print(" ") 
        print(" ") 
    # Configure the system to run once every hour
    # Start the simulation using QPEs from 4-6 hours ago
    systemStartTime = currentTime - timedelta(minutes=270) #4h,30 min
    # Save states for the current run with the current time step's timestamp
    systemStateEndTime = currentTime - timedelta(minutes=210) #4h
    # Run warm up using the last hour of data until the current time step
    systemWarmEndTime = currentTime - timedelta(minutes=240)
    # Setup the simulation forecast starting point as the current timestemp
    systemStartLRTime = currentTime
    # Run simulation for 360min (6 hours) into the future
    systemEndTime = currentTime + timedelta(minutes=360)

    # Configure failure-tolerance for missing QPE timesteps
    # Only check for states as far as we have QPFs (6 hours)
    failTime = currentTime - timedelta(hours=6)
    
    try:
        # Clean up old QPE files from GeoTIFF archive (older than 6 hours)
        #      Keep latest QPFs
        print("***_________Cleaning old QPE files from the precip folder_________***")
        cleanup_precip(currentTime, failTime, precipFolder, qpf_store_path)
        print("***_________Precip folder cleaning completed_________***")
        # Get the necessary QPEs and QPFs for the current time step into the GeoTIFF precip folder
        # store whether there's a QPE gap or the QPEs for the current time step is missing
        print(' ')
        print("***_________Retrieving IMERG files_________***")
        get_new_precip(currentTime, server, precipFolder, email, HindCastMode, qpf_store_path)
        print("***_________QPE's are complete in precip folder_________***")
        print(' ')
        #Produce ML qpf from currentTime - 4h till currentime +2h
        print(f"***_________Generating the nowcast from {currentTime - timedelta(hours=3.5)} to {currentTime + timedelta(hours=2.5)}_________***")
        run_ml_nowcast(currentTime,precipFolder, nowcast_model_name)
        print("***_________Al QPE + QPF files are ready in local folder_________***")
    except:
        print("There was a problem with the QPE routines. Ignoring errors and continuing with execution")
           
    #copying precip files into folder 
    rename_ef5_precip(precipEF5Folder,precipFolder)       
    # Check to see if all the states for the current time step are available: ["crest_SM", "kwr_IR", "kwr_pCQ", "kwr_pOQ"]
    # If not then search for previous ones
    print(" ")
    print("***_________Preparing the Ef5 run_________***")
    foundAllStates = False
    realSystemStartTime = systemStartTime
    print("    Looking for states.")
    # Iterate over all necessary states and check if they're available for the current run
    # Only go back up to 6 hours, in 30min decrements
    while foundAllStates == False and realSystemStartTime > failTime:
        foundAllStates = True
        for state in modelStates:
            if is_non_zero_file(statesPath + state + "_" + realSystemStartTime.strftime("%Y%m%d_%H%M") + ".tif") == False:
                print('    Missing start state: ' + statesPath + state + '_' + realSystemStartTime.strftime("%Y%m%d_%H%M") + '.tif')
                foundAllStates = False
        if foundAllStates == False:
            realSystemStartTime = realSystemStartTime - timedelta(minutes=30)  
        
    # If no states are found for the last 6 hours, assume that no previous states exist, and
    # use the current time step as the starting point for a "cold" start.
    # If notifications are enabled, notify all recipients about not finding states.    
    if not foundAllStates:
        if SEND_ALERTS:
            subject = systemName + ' failed for ' + currentTime.strftime("%Y%m%d_%H%M")
            message = 'Missing states from ' + realSystemStartTime.strftime("%Y%m%d_%H%M") + ' to ' + systemStartTime.strftime("%Y%m%d_%H%M") + '. Starting model with cold states.'    
        # for recipient in alert_recipients:
        #         send_mail(smtp_server, smtp_port, account_address, account_password, alert_sender, recipient, subject, message)
        print('    No states found!!!')
        realSystemStartTime = systemStartTime
    # If notifications are enabled, notify if no immediately anteceding states existed,
    # and had to use old states.
    elif realSystemStartTime != systemStartTime:
        if SEND_ALERTS:
            subject = systemName + ' warning for ' + currentTime.strftime("%Y%m%d_%H%M")
            message = 'Using states from ' + realSystemStartTime.strftime("%Y%m%d_%H%M") + ' instead of ' + systemStartTime.strftime("%Y%m%d_%H%M")
            for recipient in alert_recipients:
               send_mail(smtp_server, smtp_port, account_address, account_password, alert_sender, recipient, subject, message)
        print('Had to use older states')
    
    print(" ")
    print("    Writting control file.")
    # Clean up "Hot" folders
    # Delete the previously existing "Hot" folders, ignore error if it doesn't exist
    rmtree(tmpOutput, ignore_errors=1)
    rmtree(dataPath, ignore_errors=1)
    # Create the "Hot" folder for the current run
    mkdir_p(tmpOutput)
    mkdir_p(dataPath)  
    # Create the control files for both subdomains
    # Define the control file path to create
    controlFile = tmpOutput + "WA_" + subdomain + "_" + systemModel + ".txt"
    fOut = open(controlFile, "w")

    # Create a control file with updated fields
    for line in open(templatePath + template).readlines():
        line = re.sub('{OUTPUTPATH}', tmpOutput, line)
        line = re.sub('{STATESPATH}', statesPath, line)
        line = re.sub('{TIMEBEGIN}', realSystemStartTime.strftime('%Y%m%d%H%M'), line)
        line = re.sub('{TIMEBEGINLR}', systemStartLRTime.strftime('%Y%m%d%H%M'), line)
        line = re.sub('{TIMEWARMEND}', systemWarmEndTime.strftime('%Y%m%d%H%M'), line)
        line = re.sub('{TIMESTATE}', systemStateEndTime.strftime('%Y%m%d%H%M'), line)
        line = re.sub('{TIMEEND}', systemEndTime.strftime('%Y%m%d%H%M'), line)
        line = re.sub('{SYSTEMMODEL}', systemModel, line)
        fOut.write(line)
    fOut.close()

    """
    # If data assimilation if being used for CREST, clean up previous data assimilation logs
    #To do: Verify against EF5 control file - when this functionality is needed
    if DATA_ASSIMILATION and systemModel=="crest":
        # Data assimilation output files
        for log in assimilationLogs:
            if is_non_zero_file(assimilationPath + log) == True:
                remove(assimilationPath + log)
    """
    
    print("    Running simulation system for: " + currentTime.strftime("%Y%m%d_%H%M"))
    print("    Simulations start at: " + realSystemStartTime.strftime("%Y%m%d_%H%M") + " and ends at: " + systemEndTime.strftime("%Y%m%d_%H%M") + " while state update ends at: " + systemStateEndTime.strftime("%Y%m%d_%H%M"))
    print("***_________EF5 is ready to be run_________***")
    print(ef5Path)
    
    # Run EF5 simulations
    # Prepare function arguments for multiprocess invovation of run_EF5()
    arguments = [ef5Path, tmpOutput, controlFile, "ef5.log"]
    
    # Create a thread pool of the same size as the number of control files
    tp = ThreadPool(1)
    # Run each EF5 instance asynchronously using independent threads
    tp.apply_async(run_EF5, arguments)
    
    # Wait for both processes to finish and collapse the thread pool
    tp.close()
    tp.join()
    
    print("******** EF5 Outputs are ready!!! ********")
    
          
def get_geotiff_datetime(geotiff_path):
    """Funtion that extracts a datetime object corresponding to a Geotiff's timestamp

    Arguments:
        geotiff_path {str} -- path to the geotiff to extract a datetime from

    Returns:
        datetime -- datetime object based on geotiff timestamp
    """
    geotiff_file = geotiff_path.split('/')[-1]
    geotiff_timestamp = geotiff_file.split('.')[2]
    geotiff_datetime = dt.strptime(geotiff_timestamp, '%Y%m%d%H%M')
    return geotiff_datetime

def cleanup_precip(current_datetime, failTime, precipFolder, qpf_store_path):
    """Function that cleans up the precip folder for the current EF5 run

    Arguments:
        current_datetime {datetime} -- datetime object for the current time step
        failTime {datetime} -- datetime object representing the maximum datetime in the past
        precipFolder {str} -- path to the geotiff precipitation folder
        qpf_store_path {str} -- path to the folder where QPF files are stored
    """
    qpes = []
    qpfs = []
    
    try:
        # List all precip files
        precip_files = os.listdir(precipFolder)

        # Segregate between QPEs and QPFs
        for file in precip_files:
            if "qpe" in file:
                qpes.append(file)
            elif "qpf" in file:
                qpfs.append(file)

        print("    Deleting all QPE files older than Fail Time: ", failTime - timedelta(hours=3.5))
        for qpe in qpes:
            try:
                geotiff_datetime = get_geotiff_datetime(precipFolder + qpe)
                if geotiff_datetime < failTime - timedelta(hours=3.5):
                    os.remove(precipFolder + qpe)
            except Exception as e:
                print(f"Error processing QPE file {qpe}: {e}")

        print("    Deleting all QPF files older than Current Time: ", current_datetime)
        print("    Copying all QPF files older than Current Time: ", current_datetime, " into qpf_store folder.")
        for qpf in qpfs:
            try:
                geotiff_datetime = get_geotiff_datetime(precipFolder + qpf)
                if geotiff_datetime < current_datetime:
                    shutil.copy2(precipFolder + qpf, qpf_store_path)
                os.remove(precipFolder + qpf)
            except Exception as e:
                print(f"Error processing QPF file {qpf}: {e}")

        print(f"    Deleting all QPE files newer than Current Time: {current_datetime - timedelta(hours=4)} because it might be duplicated files")
        for qpedup in qpes:
            try:
                geotiff_datetime = get_geotiff_datetime(precipFolder + qpedup)
                if geotiff_datetime > current_datetime - timedelta(hours=4):
                    os.remove(precipFolder + qpedup)
            except Exception as e:
                print(f"Error processing QPE duplicate file {qpedup}: {e}")

        print(f"    Deleting all QPF files in store folder older than: {current_datetime - timedelta(hours=4)}")
        qpf_stored_files = os.listdir(qpf_store_path)
        qpf_stored_files = [f for f in qpf_stored_files if f.endswith('.tif')]
        max_qpf = current_datetime - timedelta(hours=4)
        for qpf_stored in qpf_stored_files:
            try:
                qpf_datetime = get_geotiff_datetime(qpf_store_path + qpf_stored)
                if qpf_datetime < max_qpf:
                    os.remove(qpf_store_path + qpf_stored)
            except Exception as e:
                print(f"Error processing stored QPF file {qpf_stored}: {e}")
    except Exception as e:
        print(f"General error in cleanup_precip function: {e}")
    
    
def extract_timestamp(filename):
    date_str = filename.split('.')[4][:8]  
    time_str = filename.split('-')[3][1:]  
    date_time_str = date_str + time_str
    final_datetime = datetime.datetime.strptime(date_time_str, '%Y%m%d%H%M%S')+timedelta(minutes=30)
    return final_datetime
            
def retrieve_imerg_files(url, email, HindCastMode, date):
    if HindCastMode:
        folder = date.strftime('%Y/%m/')
        url_server = url + '/' + folder
    else: 
        folder = date.strftime('%Y/%m/')
        url_server = url + '/' + folder
        #url_server = url
    # Send a GET request to the URL
    response = requests.get(url_server, auth=(email, email))

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the content of the response with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all links on the page
        links = soup.find_all('a')

        # Extract file names from the links
        files = [link.get('href') for link in links if link.get('href').endswith('30min.tif')]
    else:
        print(f"Failed to retrieve the directory listing. Status code: {response.status_code}")
        
    return files 

def get_gpm_files(precipFolder, initial_timestamp, final_timestamp, ppt_server_path ,email):
    #path server
    server = ppt_server_path
    file_prefix = '3B-HHR-E.MS.MRG.3IMERG.'
    file_suffix = '.V07B.30min.tif'
    
    final_date = final_timestamp + timedelta(minutes=30)
    delta_time = datetime.timedelta(minutes=30)
    
    # Loop through dates
    current_date = initial_timestamp
    #acumulador_30M = 0
    
    while (current_date < final_date):
        initial_time_stmp = current_date.strftime('%Y%m%d-S%H%M%S')
        final_time = current_date + DT.timedelta(minutes=29)
        final_time_stmp = final_time.strftime('E%H%M59')
        final_time_gridout = current_date + DT.timedelta(minutes=30)
        folder = current_date.strftime('%Y/%m/')
        
        # #finding accum
        hours = (current_date.hour)
        minutes = (current_date.minute)
    
        # # Calculate the number of minutes since the beginning of the day.
        total_minutes = hours * 60 + minutes
    
        date_stamp = initial_time_stmp + '-' + final_time_stmp + '.' + f"{total_minutes:04}"

        filename = folder + file_prefix + date_stamp + file_suffix

        print('    Downloading ' + final_time_gridout.strftime('%Y-%m-%d %H:%M'))
        try:
            # Download from NASA server
            get_file(filename,server,email)
            # Process file for domain and to fit EF5
            # Filename has final datestamp as it represents the accumulation upto that point in time
            gridOutName = precipFolder+'imerg.qpe.' + final_time_gridout.strftime('%Y%m%d%H%M') + '.30minAccum.tif'
            local_filename = file_prefix + date_stamp + file_suffix
            NewGrid, nx, ny, gt, proj = processIMERG(local_filename,xmin,ymin,xmax,ymax)
            filerm = file_prefix + date_stamp + file_suffix
            # Write out processed filename
            WriteGrid(gridOutName, NewGrid, nx, ny, gt, proj)
            os.remove(filerm)
        except Exception as e:
            print(e)
            print(filename)
            pass

        # Advance in time
        current_date = current_date + delta_time
          
def get_file(filename,server,email):
   ''' Get the given file from jsimpsonhttps using curl. '''
   url = server + '/' + filename
   cmd = 'curl -sO -u ' + email + ':' + email + ' ' + url
   args = cmd.split()
   process = subprocess.Popen(args, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
   process.wait() # wait so this program doesn't end before getting all files#
   
def ReadandWarp(gridFile,xmin,ymin,xmax,ymax):

    #Read grid and warp to domain grid
    #Assumes no reprojection is necessary, and EPSG:4326
    rawGridIn = gdal.Open(gridFile, GA_ReadOnly)

    # Adjust grid
    pre_ds = gdal.Translate('OutTemp.tif', rawGridIn, options="-co COMPRESS=Deflate -a_nodata 29999 -a_ullr -180.0 90.0 180.0 -90.0")

    gt = pre_ds.GetGeoTransform()
    proj = pre_ds.GetProjection()
    nx = pre_ds.GetRasterBand(1).XSize
    ny = pre_ds.GetRasterBand(1).YSize
    NoData = 29999
    pixel_size = gt[1]

    #Warp to model resolution and domain extents
    ds = gdal.Warp('', pre_ds, srcNodata=NoData, srcSRS='EPSG:4326', dstSRS='EPSG:4326', dstNodata='29999', format='VRT', xRes=pixel_size, yRes=-pixel_size, outputBounds=(xmin,ymin,xmax,ymax))

    WarpedGrid = ds.ReadAsArray()
    new_gt = ds.GetGeoTransform()
    new_proj = ds.GetProjection()
    new_nx = ds.GetRasterBand(1).XSize
    new_ny = ds.GetRasterBand(1).YSize

    return WarpedGrid, new_nx, new_ny, new_gt, new_proj

def WriteGrid(gridOutName, dataOut, nx, ny, gt, proj):
    #Writes out a GeoTIFF based on georeference information in RefInfo
    driver = gdal.GetDriverByName('GTiff')
    dst_ds = driver.Create(gridOutName, nx, ny, 1, gdal.GDT_Float32, ['COMPRESS=DEFLATE'])
    dst_ds.SetGeoTransform(gt)
    dst_ds.SetProjection(proj)
    dataOut.shape = (-1, nx)
    dst_ds.GetRasterBand(1).WriteArray(dataOut, 0, 0)
    dst_ds.GetRasterBand(1).SetNoDataValue(-9999.0)
    dst_ds = None

def processIMERG(local_filename,llx,lly,urx,ury):
    # Process grid
    # Read and subset grid
    NewGrid, nx, ny, gt, proj = ReadandWarp(local_filename,llx,lly,urx,ury)
    # Scale value
    NewGrid = NewGrid*0.1
    return NewGrid, nx, ny, gt, proj

def extract_datetime_from_filename(filename):
    base_name = os.path.basename(filename)
    date_str = base_name.split('.')[2]  # Get YYYYMMDDHHMM part
    filename = datetime.datetime.strptime(date_str, '%Y%m%d%H%M')
    return filename
                             
def get_new_precip(current_timestamp, ppt_server_path, precipFolder, email, HindCastMode, qpf_store_path):
    """Function that brings latest IMERG precipitation file into the GeoTIFF precip folder

    Arguments:
        current_timestamp {datetime} -- current time step's timestamp
        netcdf_feed_path {str} -- path to the geoTIFF precip data feed --- el httml
        geotiff_precip_path {str} -- path to the GeoTIFF precip archive -- el folder precip 

    Returns:
        ahead {bool} -- Returns True if the latest GeoTIFF timestamp is agead of the current time step
        gap {bool} -- Returns True if there is a gap larger than 30min between the latest GeoTIFF timestamp and the current time step
        exists {bool} -- Returns True there is a GeoTIFF file in the archive for the current time step
    """
    #Obtainign the lates time step in the imerg server 
    # server_files = retrieve_imerg_files(ppt_server_path, email, HindCastMode, )
    # most_recent_IMERG = max(server_files, key=lambda x: datetime.datetime.strptime(x[23:39], '%Y%m%d-S%H%M%S'))
    # formatted_latest_imerg = datetime.datetime.strptime(most_recent_IMERG[23:39], '%Y%m%d-S%H%M%S')  #last imerg on server available
    
    #Look for the most recent file in precip folder
    #Obtainign the latest time step in the folder
    files_folder = os.listdir(precipFolder)
    tif_files = [f for f in files_folder if "qpe" in f]
    
    #the first hour of nowcast files will be current time - 3.5h
    nowcast_older = current_timestamp - timedelta(hours = 3.5) #This is the first nowcast file to be created 
    if tif_files:
        print("    There are IMERG files in the precip folder")
        # Extract the most recent date from files
        latest_date = max(tif_files, key=lambda x: datetime.datetime.strptime(x[10:22], '%Y%m%d%H%M'))
        formatted_latest_pptfile = datetime.datetime.strptime(latest_date[10:22], '%Y%m%d%H%M') #last file on precip
        #if the latest imerg file in folder corresponds to the older nowcast file (current time - 4h)
        if formatted_latest_pptfile < nowcast_older:
            # and if the time difference betwen the current timestep and the latest imerg in folder is less than 30 min.
            if nowcast_older - formatted_latest_pptfile <= timedelta(minutes=60):
                print(f"    There are less than 60 min between last imerg file available on folder: {formatted_latest_pptfile} and last imerg file on server: ", nowcast_older-timedelta(minutes=30))
                #List the missing dates between lastest ppt file and current timestep -4h
                missing_dates = []
                # Iterar desde la fecha del archivo más reciente hasta el timestamp actual en intervalos de 30 minutos
                next_timestamp = formatted_latest_pptfile + timedelta(minutes=30)
                while next_timestamp < nowcast_older:
                    missing_dates.append(next_timestamp)
                    next_timestamp += timedelta(minutes=30)
                for date in missing_dates:
                    #Verifying if missing dates are on the GPM server.
                    server_files = retrieve_imerg_files(ppt_server_path, email, HindCastMode, date)
                    timestamps = [extract_timestamp(file) for file in server_files]
                    if date in timestamps:
                        print("    Downloading the last file of precip data")
                        #downloading the file 
                        date_server = date - timedelta(minutes=30)
                        nowcast_older_server = nowcast_older - timedelta(minutes=60) #this is because get imerg files sums up 30 min
                        get_gpm_files(precipFolder, date_server, nowcast_older_server, ppt_server_path, email)
                    else:
                        print("    The file required is not available on the IMERG server.")
                        print("    Copying the corresponding file from nowcast store folder")
                        formatted_date = date.strftime('%Y%m%d%H%M')
                        # Look for the filename in qpf store that cointains the 'formatted_timestamp' missing
                        for filename in os.listdir(qpf_store_path):
                            if formatted_date in filename:
                                source_file = os.path.join(qpf_store_path, filename)
                                destination_file = os.path.join(precipFolder, filename)
                                # Copiar el archivo al directorio de destino
                                shutil.copy2(source_file, destination_file)
                                print(f"    File '{filename}' was copied in '{precipFolder}'")
                                break                          
            else: 
                print(f"    There's more than a 60 min gap between {nowcast_older-timedelta(minutes=30)} and the latest geoTIFF file {formatted_latest_pptfile}")
                print("    Latest Geotiff file available in folder:", formatted_latest_pptfile)
                print("    Last IMERG file to download:", nowcast_older - timedelta(minutes=30))
                #Downloading imerg files between dates
                nowcast_older_server = nowcast_older - timedelta(minutes=60)
                latest_pptfile = formatted_latest_pptfile
                get_gpm_files(precipFolder, latest_pptfile, nowcast_older_server, ppt_server_path, email)
                
                #List the missing dates between latest ppt file and current timestep
                missing_dates = []
                next_timestamp = formatted_latest_pptfile + timedelta(minutes=30)
                while next_timestamp < nowcast_older:
                    missing_dates.append(next_timestamp)
                    next_timestamp += timedelta(minutes=30)
               
                for date in missing_dates: 
                    #retrieven file names from GPM server
                    server_files = retrieve_imerg_files(ppt_server_path, email, HindCastMode, date)    
                    timestamps = [extract_timestamp(file) for file in server_files]
                    
                    #Looking for timestaps missing in imerg
                    if date not in timestamps:
                        print(f"    File {date} is missing")
                        print("    Copying the corresponding file from nowcast store folder")
                        formatted_date = date.strftime('%Y%m%d%H%M')
                        # Copying missing file from qpf store folder 
                        for filename in os.listdir(qpf_store_path):
                            if formatted_date in filename:
                                source_file = os.path.join(qpf_store_path, filename)
                                destination_file = os.path.join(precipFolder, filename)
                                # Copying file to precip folder
                                shutil.copy2(source_file, destination_file)
                                print(f"    File '{filename}' was copied in '{precipFolder}'")
                            else:
                                break
                    #if date is in timestaps, file is available.    
    else:
        print("    No '.tif' files found in the precip folder.") 
        #If there is no files in folder, Download the entire chuck of dates 
        #from failtime (current time - 6h) to Nowcast time (current time -4h) 
        initial_time = current_timestamp - timedelta(hours = 9.5) #sames as fail time
        #Downloading imerg Files
        nowcast_older_server = nowcast_older - timedelta(minutes=60)
        initial_time_server = initial_time - timedelta(minutes=30)
        print("    Last IMERG file to download:", nowcast_older- timedelta(minutes=30))
        print("    Initial time to download:", initial_time)
        get_gpm_files(precipFolder, initial_time_server, nowcast_older_server, ppt_server_path, email)
        #if some file is missing
        missing_dates = []
        next_timestamp = initial_time + timedelta(minutes=30)

        #retrieving gpm files for the last file that it is supposed to be downloaded.
        date_in_server = nowcast_older- timedelta(minutes=30)
        server_files = retrieve_imerg_files(ppt_server_path, email, HindCastMode, date_in_server)

        while next_timestamp < nowcast_older:
            missing_dates.append(next_timestamp)
            next_timestamp += timedelta(minutes=30)
            
            for date in missing_dates:     
                timestamps = [extract_timestamp(file) for file in server_files]
                
                if date not in timestamps:
                    print(f"    File {date} is missing")
                    print("    Copying the corresponding file from nowcast store folder")
                    formatted_date = date.strftime('%Y%m%d%H%M')
                    for filename in os.listdir(qpf_store_path):
                        if formatted_date in filename:
                            source_file = os.path.join(qpf_store_path, filename)
                            destination_file = os.path.join(precipFolder, filename)
                            # Copying file to precip folder
                            shutil.copy2(source_file, destination_file)
                            print(f"    File '{filename}' was copied in '{precipFolder}'")
                        else:
                            break
                    """
                    print(f"   There is no file in qpf store with date: '{formatted_date}'") ### TO DO
                    tif_files = glob.glob(os.path.join(precipFolder, "imerg.qpe.*.30minAccum.tif"))
                    if tif_files:
                        # Find the most recent file
                        latest_file = max(tif_files, key=extract_datetime_from_filename)
                        print(f"    Latest file: {latest_file}")
                        new_filename = os.path.join(precipFolder, f"imerg.qpe.{formatted_date}.30minAccum.tif")
                        shutil.copy2(latest_file, new_filename)
                        print(f"    Created duplicate file: {new_filename}")
                    else:
                        print("    No .tif files found in precipFolder to copy")   
                    """
    # Get a list of all .tif files in the current directory and delete this files
    try:
        tif_files = glob.glob("./*.tif")
        for tif_file in tif_files:
            os.remove(tif_file)
    except:
        print(' ')

def extract_timestamp_2(filename):
    try:
        date_str = filename.split('.')[2]
        return datetime.datetime.strptime(date_str, '%Y%m%d%H%M')
    except (IndexError, ValueError):
        return None
    
def run_ml_nowcast(currentTime,precipFolder, nowcast_model_name):
    #running nowcast codes
    metadata_folder_location = 'ML/servir_nowcasting_examples/temp/imerg_geotiff_meta.json'

    try:
        tif2h5py(precipFolder, 'ML/servir_nowcasting_examples/temp/input_imerg.h5', metadata_folder_location,
            x1=xmin, y1=ymin, x2=xmax, y2=ymax)
        
        # with library implementation
        param_dict = load_default_params_for_model(nowcast_model_name)
        param_dict['output_h5_fname'] = 'ML/servir_nowcasting_examples/temp/output_imerg.h5'
    
        # optionally modify the parameter dictionary
        nowcast(param_dict)

        ### Command 3: python m_h5py2tif.py
        # with library implementation
        h5py2tif('ML/servir_nowcasting_examples/temp/output_imerg.h5', 
                metadata_folder_location, 
                precipFolder, 
                num_predictions = 1,
                method=nowcast_model_name)

    except Exception as e:
        print("    Something failed within ML-nowcast routines with exception {} . Execution has been paused.".format(e))
        print(e)
        
        #Produce ML qpf from currentTime - 4h till currentime +2h
        init = currentTime - timedelta(hours = 3.5)
        final = currentTime + timedelta(hours = 2.5)
        print('    Duplicating last qpe file')
        date_list = []
        current_date = init
        while current_date <= final:
            date_list.append(current_date.strftime('%Y%m%d%H%M'))
            current_date += timedelta(minutes=30)
            
        # Find all .tif files in the directory
        tif_files = glob.glob(os.path.join(precipFolder, "imerg.qpe.*.30minAccum.tif"))
    
        # Extract dates from filenames and find the most recent file
        most_recent_file = None
        most_recent_date = None

        for file in tif_files:
            filename = os.path.basename(file)
            file_date_str = filename.split('.')[2]
            file_date = datetime.datetime.strptime(file_date_str, '%Y%m%d%H%M')
            if most_recent_date is None or file_date > most_recent_date:
                most_recent_date = file_date
                most_recent_file = file

        if most_recent_file is None:
            print("     No valid .tif files found in the directory.")
        else:
            print(f"     Most recent file selected: {most_recent_file}")

        # Duplicate the most recent file with new names based on the date list
        for date_str in date_list:
            new_filename = f"imerg.qpe.{date_str}.30minAccum.tif"
            new_filepath = os.path.join(precipFolder, new_filename)
            shutil.copy2(most_recent_file, new_filepath)
            print(f"Created file: {new_filepath}")    


def send_mail(smtp_server, smtp_port, account_address, account_password, sender, to, subject, text):
    """Function to send error emails

    Arguments:
        to {str} -- destination email address
        subject {str} -- email subject
        text {str} -- email message contents
    """
    msg = MIMEMultipart()

    msg['From'] = sender
    msg['To'] = to
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    mailServer = smtplib.SMTP(smtp_server, smtp_port)
    mailServer.ehlo()
    mailServer.starttls()
    mailServer.ehlo()
    mailServer.login(account_address, account_password)
    mailServer.sendmail(account_address, to, msg.as_string())
    mailServer.close()
    
def is_non_zero_file(fpath):
    """Function that checks if a file exists and is not empty

    Arguments:
        fpath {str} -- file path to check

    Returns:
        bool -- True or False
    """
    if os.path.isfile(fpath) and os.path.getsize(fpath) > 0:
        return True
    else:
        return False
    
def mkdir_p(path):
    """Function that makes a new directory.

    This function tries to make directories, ignoring errors if they exist.

    Arguments:
        path {str} -- path of folder to create
    """
    try:
        makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise

def run_EF5(ef5Path, hot_folder_path, control_file, log_file):
    """Run EF5 as a subprocess call

    Based on the command:

        subprocess.call(ef5Path + " " + tmpOutput + "flash"+config_file.abbreviation+"_" + systemModel + ".txt >" + tmpOutput + "ef5.log", shell=True)

    Arguments:
        ef5Path {str} -- Path to EF5 binary
        hot_folder_path {str} -- Path to the current run's "hot" foler
        control_file {str} -- path to the control file fir the simulation
        log_file {str} -- path to the log file for this run
    """
    subprocess.call(ef5Path + " " + control_file + " > " + hot_folder_path + log_file, shell=True)

def rename_ef5_precip(precipEF5Folder, precipFolder):    
    for filename in os.listdir(precipFolder):
        if filename.endswith('.tif'):
            source_file = os.path.join(precipFolder, filename)
            dest_file = os.path.join(precipEF5Folder, filename)
            try:
                shutil.copy(source_file, dest_file)
            except PermissionError as e:
                print(f"PermissionError: {e}")
    for filename2 in os.listdir(precipEF5Folder):
        if 'qpf' in filename2 and filename2.endswith('.tif'):
            new_filename = filename2.replace('qpf', 'qpe')
            source_file = os.path.join(precipEF5Folder, filename2)
            dest_file = os.path.join(precipEF5Folder, new_filename)
            try:
                os.rename(source_file, dest_file)
            except PermissionError as e:
                print(f"PermissionError: {e}")
             
"""
Run the main() function when invoked as a script
"""
if __name__ == "__main__":
    main(sys.argv)

