import os
import glob
import shutil
import datetime
from datetime import datetime
from datetime import timedelta
import subprocess
from servir.scripts.m_nowcasting import load_default_params_for_model, nowcast
from servir.utils.m_h5py2tif import h5py2tif
from servir.utils.m_tif2h5py import tif2h5py


def run_ml_nowcast(currentTime, precipFolder, nowcast_model_name, xmin, ymin, xmax, ymax):
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

        ## This is temporal:
        [shutil.move(os.path.join(precipFolder, nowcast_model_name, f), os.path.join(precipFolder, f)) for f in os.listdir(os.path.join(precipFolder, nowcast_model_name))]
        subprocess.run(["rm", "-rf", f"{precipFolder}/{nowcast_model_name}"])


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
            file_date = datetime.strptime(file_date_str, '%Y%m%d%H%M')
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

