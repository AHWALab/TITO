"""
Real-time model/subdomain execution script

This is an IMERG-based operational system that integrates either Ml routines or 
NWP outputs from public available sources to produce a flash flood forecast in real time. 


Contributors:
Vanessa Robledo - vrobledodelgado@uiowa.edu
Humberto Vergara - humberto-vergaraarrieta@uiowa.edu
V.2.0 - October 01, 2025

Please use this script and a configuration file as follows:

    $> python orchestrator.py <configuration_file.py>

"""

from shutil import rmtree, copy
import os
from os import makedirs, listdir, rename, remove
import glob
from datetime import datetime as dt
from datetime import timedelta
import numpy as np
import re
import subprocess
import sys
from tito_utils.file_utils import cleanup_precip, newline
from tito_utils.qpe_utils import get_new_precip
from tito_utils.qpf_utils import run_convlstm, download_GFS, GFS_searcher, WRF_searcher 
from tito_utils.ef5 import prepare_ef5, run_ef5_simulation
print(">>> Modules imported")

"""
Setup Environment Variables for Linux Shared Libraries and OpenMP Threads (PARA USAR ML de AGRHYMET)

"""

def main(args):
    """Main function of the script.
    
    This function reads the real-time configuration script, makes sure the necessary files to run EF5 exist and are in the right place, runs the model(s), writes the outputs and states, and reports vie email if an error occurs during execution.
    
    Arguments:
        args {list} -- the first argument ([1]) corresponds to a real-time configuration file.
    """
    ###-------------------------- SETTING SECTION --------------------------------
    #set true of False to fill 4h imerg latency and create +2h hours (nowcast)
    NOWCAST = True 
    
    # Read the configuration file User should change this line if the configuration file has a different name
    import westafrica1km_config as config_file
    print(">>> Config file loaded")

    #Configuration file
    domain = config_file.domain
    subdomain = config_file.subdomain
    xmin = config_file.xmin
    ymin = config_file.ymin
    xmax = config_file.xmax
    ymax = config_file.ymax
    systemModel = config_file.systemModel
    systemName = config_file.systemName
    systemTimestep = config_file.systemTimestep
    ef5Path = config_file.ef5Path
    precipFolder = config_file.precipFolder
    statesPath = config_file.statesPath
    precipEF5Folder = config_file.precipEF5Folder
    modelStates = config_file.modelStates
    templatePath = config_file.templatePath
    template = config_file.templates
    nowcast_model_name = config_file.nowcast_model_name
    dataPath = config_file.dataPath
    qpf_store_path = config_file.qpf_store_path
    tmpOutput = config_file.tmpOutput
    SEND_ALERTS = config_file.SEND_ALERTS
    alert_recipients = config_file.alert_recipients
    HindCastMode = config_file.HindCastMode
    HindCastDate = config_file.HindCastDate
    LR_run = config_file.run_LR
    LR_TimeStep = config_file.LR_timestep
    GFS_archive_path = config_file.QPF_archive_path
    email_gpm = config_file.email_gpm
    server = config_file.server
    smtp_config = {
        'smtp_server': config_file.smtp_server,
        'smtp_port': config_file.smtp_port,
        'account_address': config_file.account_address,
        'account_password': config_file.account_password,
        'alert_sender': config_file.alert_sender}
    
    newline(2)
    
    # Real-time mode or Hindcast mode
    # Figure out the timing for running the current timestep
    if HindCastMode == True:
        currentTime = dt.strptime(HindCastDate, "%Y-%m-%d %H:%M")
    else:
        currentTime = dt.datetime.now(dt.timezone.utc)
    
    # Round down the current minutess to the nearest 30min increment in the past (for 30 forecast)
    if systemTimestep == 30:
        minutes = int(np.floor(currentTime.minute / 30.0) * 30)
    if systemTimestep == 60: #for 60 min forecast
        minutes = 0 
    # Use the rounded down minutes as the timestamp for the current time step
    currentTime = currentTime.replace(minute=minutes, second=0, microsecond=0)
    
    if HindCastMode == True:
        print(f"*** Starting hindcast run cycle at {currentTime.strftime("%Y-%m-%d_%H:%M")} UTC ***")
        newline(2)
    else:
        print(f"*** Starting real-time run cycle at {currentTime.strftime("%Y-%m-%d_%H:%M")} UTC ***")
        newline(2) 
        
    # Configure the system to run once every hour
    # Start the simulation using QPEs from 4-6 hours ago
    systemStartTime = currentTime - timedelta(hours=4.5) 
    # Save states for the current run with the current time step's timestamp
    systemStateEndTime = currentTime - timedelta(hours=4) #change to 4
    # Run warm up using the last hour of data until the current time step
    systemWarmEndTime = currentTime - timedelta(hours=4)
    # Only check for states as far as we have QPs (6 hours)
    failTime = currentTime - timedelta(hours=6)
    
    systemStartLRTime = dt.strptime(config_file.StartLRtime,"%Y-%m-%d %H:%M")
    EndLRTime = dt.strptime(config_file.EndLRTime,"%Y-%m-%d %H:%M")
    
    if HindCastMode and LR_run:
        systemEndTime = EndLRTime + timedelta(hours=4) #4 hours dry
    if HindCastMode and not LR_run:
        systemEndTime = currentTime + timedelta(hours=6) #4 hours dry after ml
    #operational options
    if not HindCastMode and LR_run:
        systemStartLRTime = currentTime + timedelta(hours=2) #change as desired
        EndLRTime = currentTime + timedelta(hours=6) #4 hours of qpf
        systemEndTime = EndLRTime + timedelta(hours=4) #4 hours dry after gfs
    if not HindCastMode and not LR_run:
        systemEndTime = currentTime + timedelta(hours=6) #si no corro gfs y hindcast no
        
    ###-------------------------- START ROUTINES --------------------------------
    try:
        # Clean up old QPE files from GeoTIFF archive (older than 6 hours)
        # Keep latest QPFs
        print("***_________Cleaning old QPE files from the precip folder_________***")
        cleanup_precip(currentTime, precipFolder, qpf_store_path)
        newline(1)
        print("***_________Precip folder cleaning completed_________***")
        newline(2)
        
        # Get the necessary QPEs and QPFs for the current time step into the GeoTIFF precip folder store whether there's a QPE gap or the QPEs for the current time step is missing
        print("***_________Retrieving IMERG files_________***")
        get_new_precip(currentTime, server, precipFolder, email_gpm, HindCastMode, qpf_store_path, xmin, ymin, xmax, ymax)
        newline(1)
        print("***_________IMERG files are complete in precip folder_________***")
        newline(2)
    except:
        print("There was a problem with the QPE routines. Ignoring errors and continuing with execution")
        
    ###-------------------------- START NOWCAST SECTION --------------------------------      
    if NOWCAST:
        try:
            #if true, will create a nowcast filling the last 4 hours of imerge latency + 2hours of nowcast 
            print(f"***_________Generating the nowcast from {currentTime - timedelta(hours=3.5)} to {currentTime + timedelta(hours=2.5)}_________***")
            run_convlstm(currentTime, precipFolder, nowcast_model_name, xmin, ymin, xmax, ymax)
            newline(1)
            print("***_________Nowcast/ML files are complete in precip folder_________***")
            newline(2)
        except:
            print("There was a problem with the ML routines. Ignoring errors and continuing with execution")
            
    ###-------------------------- START LR-QPF SECTION --------------------------------
    if LR_run:
        print(f"***_________Preparing QPF from {systemStartLRTime} to {systemEndTime}")
        try:
            #looking for files on archive and copy the needed ones on qpf_store/gfs_data
            GFS_searcher(GFS_archive_path, qpf_store_path, systemStartLRTime, EndLRTime, xmin, xmax, ymin, ymax)
            
            # #TO USE WRF FILES DEFINE:
            # var_name = "PREC_ACC_C"
            # filename_template = "PREC_d01_YYYY-MM-DD_HH_mm_SS.nc"
            # WRF_archive_path = config_file.QPF_archive_path
            # WRF_searcher(WRF_archive_path, qpf_store_path, systemStartLRTime, EndLRTime, LR_timestep, var_name, filename_template)
        except:
            ("There was a problem with the QPF routines. Ignoring errors and continuing with execution")
        newline(1)
        print("***_________All QPE + QPF files are ready in local folder_________***")
    newline(2)
    
    ###-------------------------- START EF5 SECTION --------------------------------
    print("***_________Preparing the EF5 run_________***")
    realSystemStartTime, controlFile = prepare_ef5(precipEF5Folder, precipFolder, statesPath, modelStates, 
        systemStartTime, failTime, currentTime, systemName, SEND_ALERTS, 
        alert_recipients, smtp_config, tmpOutput, dataPath, 
        subdomain, systemModel, templatePath, template, systemStartLRTime, 
        systemWarmEndTime, systemStateEndTime, systemEndTime, LR_TimeStep, LR_run)
    
    print(f"    Running simulation system for: {currentTime.strftime("%Y%m%d_%H%M")}")
    print(f"    Simulations start at: {realSystemStartTime.strftime("%Y%m%d_%H%M")} and ends at: {systemEndTime.strftime("%Y%m%d_%H%M")} while state update ends at: {systemStateEndTime.strftime("%Y%m%d_%H%M")}")
    
    print("***_________EF5 is ready to be run_________***")
    
    run_ef5_simulation(ef5Path, tmpOutput, controlFile)
    newline(2)
    print("******** EF5 Outputs are ready!!! ********")
             
"""
Run the main() function when invoked as a script
"""
if __name__ == "__main__":
    main(sys.argv)

