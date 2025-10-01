domain = "WestAfrica"
subdomain = "Regional"
model_resolution = "1km"
systemModel = "crest"
systemTimestep = 60 #in minutes

# Coordinates used for generating Nowcast / QPF files.
# For ML-based nowcasting, these coordinates should cover a region of size 518 x 360 pixels.
xmin = -21.4
xmax = 30.4
ymin = -2.9
ymax = 33.1
nowcast_model_name = "convlstm" 
systemName = systemModel.upper() + " " + domain.upper() + " " + subdomain.upper()
ef5Path = "/Shared/lss_hvergaraarrieta/tools/EF5/bin/ef5" 
statesPath = "states/"
precipFolder = "precip/"
precipEF5Folder = "precipEF5/"
modelStates = ["crest_SM", "kwr_IR", "kwr_pCQ", "kwr_pOQ"]
templatePath = "templates/"
templates = "ef5_control_template.txt"
dataPath = "outputs/"
qpf_store_path = 'qpf_store/'
tmpOutput = dataPath + "tmp_output_" + systemModel + "/"

#Alerts configuration
SEND_ALERTS = False
smtp_server = "smtp.gmail.com"
smtp_port = 587
account_address = "model_alerts@gmail.com"
account_password = "supersecurepassword9000"
alert_sender = "Real Time Model Alert" # can also be the same as account_address
alert_recipients = ["fixer1@company.com", "fixer2@company.com", "panic@company.com",...]
copyToWeb = False

#Simulation times 
"""
If Hindcast and LR_mode is True, user MUST define StartLRtime, EndLRTime, LR_timestep,GFS_archive_path
If running in operational mode (Hindcast False) and LR_mode = True, user only have to define LR_timestep, GFS_archive_path
"""
HindCastMode = False 
HindCastDate = "2024-07-04 09:00" #"%Y-%m-%d %H:%M" UTC

run_LR = False
StartLRtime = "2024-07-04 11:00" #"%Y-%m-%d %H:%M" UTC. Date of first QPF file
EndLRTime = "2024-07-04 18:00" #"%Y-%m-%d %H:%M" UTC. Date of last QPF file
LR_timestep = "60u"
QPF_archive_path = "qpf_store/archive/"

# Email associated to GPM account
email_gpm = 'vrobledodelgado@uiowa.edu'
server = 'https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/early/'
