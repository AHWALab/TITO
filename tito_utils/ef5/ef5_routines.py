import os
import shutil
import re
import glob
from shutil import rmtree
import datetime
from datetime import timedelta
from multiprocessing.pool import ThreadPool
import subprocess
from tito_utils.file_utils.file_handling import is_non_zero_file, mkdir_p
from tito_utils.ef5.alerts import send_mail

def rename_ef5_precip(precipEF5Folder, precipFolder): 
    """
    Move the qpe and qpf files into precipEF5folder to be ingested by EF5 using
    a unify format.
    """   
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


def find_available_states(statesPath, modelStates, systemStartTime, failTime):
    """
    Look for the set of most recent states available.
    
    """
    foundAllStates = False
    realSystemStartTime = systemStartTime

    print("    Looking for states.")

    # Iterate over all necessary states and check if they're available for the current run
    # Only go back up to 6 hours, in 30min decrements
    while not foundAllStates and realSystemStartTime > failTime:
        foundAllStates = True
        for state in modelStates:
            state_path = f"{statesPath}{state}_{realSystemStartTime.strftime('%Y%m%d_%H%M')}.tif"
            if not is_non_zero_file(state_path):
                print(f"    Missing start state: {state_path}")
                foundAllStates = False
        if not foundAllStates:
            realSystemStartTime -= timedelta(minutes=30)

    return foundAllStates, realSystemStartTime


def send_state_alerts(foundAllStates, realSystemStartTime, systemStartTime,
                      currentTime, systemName, SEND_ALERTS,
                      alert_recipients, smtp_config):
    """
    Sends alert emails if necessary based on the availability of model states.

    Args:
        foundAllStates (bool): whether all required states were found
        realSystemStartTime (datetime): actual start time used for the simulation
        systemStartTime (datetime): originally planned system start time
        currentTime (datetime): current system time
        systemName (str): name of the system sending the alert
        SEND_ALERTS (bool): whether to send email alerts or not
        alert_recipients (list): list of email addresses to notify
        smtp_config (dict): configuration dictionary containing:
            - smtp_server (str)
            - smtp_port (int)
            - account_address (str)
            - account_password (str)
            - alert_sender (str)
    """

    # Exit early if email alerts are disabled
    if not SEND_ALERTS:
        return

    # If no valid states were found, notify about a cold start
    if not foundAllStates:
        subject = f"{systemName} failed for {currentTime.strftime('%Y%m%d_%H%M')}"
        message = (
            f"Missing states from {realSystemStartTime.strftime('%Y%m%d_%H%M')} "
            f"to {systemStartTime.strftime('%Y%m%d_%H%M')}. Starting model with cold states."
        )
    
    # If older states had to be used, notify about it
    elif realSystemStartTime != systemStartTime:
        subject = f"{systemName} warning for {currentTime.strftime('%Y%m%d_%H%M')}"
        message = (
            f"Using states from {realSystemStartTime.strftime('%Y%m%d_%H%M')} "
            f"instead of {systemStartTime.strftime('%Y%m%d_%H%M')}."
        )
    
    # If states were found and up to date, no alert needed
    else:
        return

    # Send the email to each recipient in the list
    for recipient in alert_recipients:
        send_mail(
            smtp_server=smtp_config['smtp_server'],
            smtp_port=smtp_config['smtp_port'],
            account_address=smtp_config['account_address'],
            account_password=smtp_config['account_password'],
            sender=smtp_config['alert_sender'],
            to=recipient,
            subject=subject,
            text=message
        )

def write_control_file(tmpOutput, dataPath, subdomain, systemModel, 
    templatePath, template, statesPath, realSystemStartTime, systemStartLRTime, 
    systemWarmEndTime, systemStateEndTime, systemEndTime):
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
    return controlFile

def run_EF5(ef5Path, hot_folder_path, control_file, log_file):
    """
    Run EF5 as a subprocess call
    Arguments:
        ef5Path {str} -- Path to EF5 binary
        hot_folder_path {str} -- Path to the current run's "hot" foler
        control_file {str} -- path to the control file fir the simulation
        log_file {str} -- path to the log file for this run
    """
    subprocess.call(ef5Path + " " + control_file + " > " + hot_folder_path + log_file, shell=True)


def run_ef5_simulation(ef5Path, tmpOutput, controlFile):
    args = [ef5Path, tmpOutput, controlFile, "ef5.log"]
    tp = ThreadPool(1)
    tp.apply_async(run_EF5, args)
    tp.close()
    tp.join()
    for f in glob.glob("precipEF5/*"):
        os.remove(f)

 
def prepare_ef5(precipEF5Folder, precipFolder, statesPath, modelStates, 
    systemStartTime, failTime, currentTime, systemName, SEND_ALERTS, 
    alert_recipients, smtp_config, tmpOutput, dataPath, 
    subdomain, systemModel, templatePath, template, systemStartLRTime, 
    systemWarmEndTime, systemStateEndTime, systemEndTime):

    #copying precip files into folder 
    rename_ef5_precip(precipEF5Folder, precipFolder) 

    # Check to see if all the states for the current time step are available: ["crest_SM", "kwr_IR", "kwr_pCQ", "kwr_pOQ"]
    # If not then search for previous ones

    foundAllStates, realSystemStartTime = find_available_states(statesPath, modelStates, systemStartTime, failTime)

    # send alerts if needed 
    send_state_alerts(foundAllStates, realSystemStartTime, systemStartTime,
                      currentTime, systemName, SEND_ALERTS,
                      alert_recipients, smtp_config)
    print(" ")
    print("    Writting control file.")

    controlFile = write_control_file(tmpOutput, dataPath, subdomain, systemModel, 
    templatePath, template, statesPath, realSystemStartTime, systemStartLRTime, 
    systemWarmEndTime, systemStateEndTime, systemEndTime)

    """
    # If data assimilation if being used for CREST, clean up previous data assimilation logs
    #To do: Verify against EF5 control file - when this functionality is needed
    if DATA_ASSIMILATION and systemModel=="crest":
        # Data assimilation output files
        for log in assimilationLogs:
            if is_non_zero_file(assimilationPath + log) == True:
                remove(assimilationPath + log)
    """
    return realSystemStartTime, controlFile
