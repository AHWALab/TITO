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
import numpy as np
import re
import subprocess
import sys

from tito_utils.file_utils import cleanup_precip
from tito_utils.qpe_utils import get_new_precip
from tito_utils.qpf_utils import run_ml_nowcast
from tito_utils.ef5 import prepare_ef5, run_ef5_simulation
print(">>> EF5 modules imported")

"""
Setup Environment Variables for Linux Shared Libraries and OpenMP Threads
"""
def main(args):
    """Main function of the script.

    This function reads the real-time configuration script, makes sure the necessary files to run
    FLASH exist and are in the right place, runs the model(s), writes the outputs and states, and
    reports vie email if an error occurs during execution.

    Arguments:
        args {list} -- the first argument ([1]) corresponds to a real-time configuration file.
    """
        
    # Read the configuration file
    import westafrica1km_config as config_file
    print(">>> Config file loaded")
    
    domain = config_file.domain
    subdomain = config_file.subdomain
    xmin = config_file.xmin
    ymin = config_file.ymin
    xmax = config_file.xmax
    ymax = config_file.ymax
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
    dataPath = config_file.dataPath
    qpf_store_path = config_file.qpf_store_path
    tmpOutput = config_file.tmpOutput
    SEND_ALERTS = config_file.SEND_ALERTS
    alert_recipients = config_file.alert_recipients
    HindCastMode = config_file.HindCastMode
    HindCastDate = config_file.HindCastDate
    email_gpm = config_file.email_gpm
    server = config_file.server  
    smtp_config = {
        'smtp_server': config_file.smtp_server,
        'smtp_port': config_file.smtp_port,
        'account_address': config_file.account_address,
        'account_password': config_file.account_password,
        'alert_sender': config_file.alert_sender}
    
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
        print(f"*** Starting hindcast run cycle at {currentTime.strftime("%Y-%m-%d_%H:%M")} UTC ***")
        print(" ") 
        print(" ") 
    else:
        print(f"*** Starting real-time run cycle at {currentTime.strftime("%Y-%m-%d_%H:%M")} UTC ***")
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
        get_new_precip(currentTime, server, precipFolder, email_gpm, HindCastMode, qpf_store_path, xmin, ymin, xmax, ymax)
        print("***_________QPE's are complete in precip folder_________***")
        print(' ')
        #Produce ML qpf from currentTime - 4h till currentime +2h
        print(f"***_________Generating the nowcast from {currentTime - timedelta(hours=3.5)} to {currentTime + timedelta(hours=2.5)}_________***")
        run_ml_nowcast(currentTime,precipFolder, nowcast_model_name, xmin, ymin, xmax, ymax)
        print("***_________Al QPE + QPF files are ready in local folder_________***")
    except:
        print("There was a problem with the QPE routines. Ignoring errors and continuing with execution")

    print(" ")
    print("***_________Preparing the Ef5 run_________***")

    realSystemStartTime, controlFile = prepare_ef5(precipEF5Folder, precipFolder, statesPath, modelStates, 
        systemStartTime, failTime, currentTime, systemName, SEND_ALERTS, 
        alert_recipients, smtp_config, tmpOutput, dataPath, 
        subdomain, systemModel, templatePath, template, systemStartLRTime, 
        systemWarmEndTime, systemStateEndTime, systemEndTime)
    
    print(f"    Running simulation system for: {currentTime.strftime("%Y%m%d_%H%M")}")
    print(f"    Simulations start at: {realSystemStartTime.strftime("%Y%m%d_%H%M")} and ends at: {systemEndTime.strftime("%Y%m%d_%H%M")} while state update ends at: {systemStateEndTime.strftime("%Y%m%d_%H%M")}")
    
    print("***_________EF5 is ready to be run_________***")
    
    run_ef5_simulation(ef5Path, tmpOutput, controlFile)
    
    print("******** EF5 Outputs are ready!!! ********")
             
"""
Run the main() function when invoked as a script
"""
if __name__ == "__main__":
    main(sys.argv)

