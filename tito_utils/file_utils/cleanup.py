import os            
import shutil        
from datetime import timedelta  
from tito_utils.file_utils.datetime_utils import get_geotiff_datetime

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

        