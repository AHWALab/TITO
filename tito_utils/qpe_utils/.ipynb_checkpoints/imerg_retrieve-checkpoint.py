import requests               
from bs4 import BeautifulSoup  
import os
import glob
import shutil
import subprocess
import requests
from bs4 import BeautifulSoup
import datetime
from datetime import datetime as dt
from datetime import timedelta
from os import makedirs, listdir, rename, remove
import numpy as np
import osgeo.gdal as gdal
from osgeo.gdal import gdalconst
from osgeo.gdalconst import GA_ReadOnly
from tito_utils.file_utils.datetime_utils import extract_timestamp, extract_datetime_from_filename

def retrieve_imerg_files(url, email_gpm, HindCastMode, date):
    if HindCastMode:
        folder = date.strftime('%Y/%m/')
        url_server = url + '/' + folder
    else: 
        folder = date.strftime('%Y/%m/')
        url_server = url + '/' + folder
        
    # Send a GET request to the URL
    response = requests.get(url_server, auth=(email_gpm, email_gpm))

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


def get_gpm_files(precipFolder, initial_timestamp, final_timestamp, ppt_server_path, email_gpm, xmin, ymin, xmax, ymax):
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
        final_time = current_date + timedelta(minutes=29)
        final_time_stmp = final_time.strftime('E%H%M59')
        final_time_gridout = current_date + timedelta(minutes=30)
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
            get_file(filename,server, email_gpm)
            # Process file for domain and to fit EF5
            # Filename has final datestamp as it represents the accumulation upto that point in time
            gridOutName = precipFolder+'imerg.qpe.' + final_time_gridout.strftime('%Y%m%d%H%M') + '.30minAccum.tif'
            local_filename = file_prefix + date_stamp + file_suffix
            NewGrid, nx, ny, gt, proj = processIMERG(local_filename, xmin, ymin, xmax, ymax)
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


def get_file(filename,server, email_gpm):
   ''' Get the given file from jsimpsonhttps using curl. '''
   url = server + '/' + filename
   cmd = 'curl -sO -u ' + email_gpm + ':' + email_gpm + ' ' + url
   args = cmd.split()
   process = subprocess.Popen(args, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
   process.wait() # wait so this program doesn't end before getting all files#


def ReadandWarp(gridFile, xmin, ymin, xmax, ymax):

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

def processIMERG(local_filename,llx, lly ,urx, ury):
    # Process grid
    # Read and subset grid
    NewGrid, nx, ny, gt, proj = ReadandWarp(local_filename,llx, lly, urx, ury)
    # Scale value
    NewGrid = NewGrid*0.1
    return NewGrid, nx, ny, gt, proj

def get_new_precip(current_timestamp, ppt_server_path, precipFolder, email, HindCastMode, qpf_store_path, xmin, ymin, xmax, ymax):
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
                        get_gpm_files(precipFolder, date_server, nowcast_older_server, ppt_server_path, email, xmin, ymin, xmax, ymax)
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
                            else:   
                                break                          
            else: 
                print(f"    There's more than a 60 min gap between latency Imerg: {nowcast_older-timedelta(minutes=30)} and the latest geoTIFF file {formatted_latest_pptfile}")
                print("    Latest Geotiff file available in folder:", formatted_latest_pptfile)
                print("    Last IMERG file to download:", nowcast_older - timedelta(minutes=30))
                #Downloading imerg files between dates
                nowcast_older_server = nowcast_older - timedelta(minutes=60)
                latest_pptfile = formatted_latest_pptfile
                get_gpm_files(precipFolder, latest_pptfile, nowcast_older_server, ppt_server_path, email, xmin, ymin, xmax, ymax)
                
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
        initial_time = current_timestamp - timedelta(hours = 9.5) #same as fail time
        #Downloading imerg Files
        nowcast_older_server = nowcast_older - timedelta(minutes=60)
        initial_time_server = initial_time - timedelta(minutes=30)
        print("    Last IMERG file to download:", nowcast_older- timedelta(minutes=30))
        print("    Initial time to download:", initial_time)
        get_gpm_files(precipFolder, initial_time_server, nowcast_older_server, ppt_server_path, email, xmin, ymin, xmax, ymax)
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
