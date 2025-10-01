import os
import shutil
from datetime import datetime as dt
from datetime import timedelta
from .gfs_downloader import download_GFS
import glob

def GFS_searcher(path_gfs, qpf_store_path, start_time, end_time, xmin, xmax, ymin, ymax):
    """
    Check if GFS files exist between start_time and end_time.
    If all files are found, copy them to qpf_store_path.
    If not, adjust start_time to the nearest GFS cycle (00,06,12,18) before the given start_time
    and call download_GFS with the new start_time.

    Parameters
    ----------
    path_gfs : str
        Path where the GFS tif files are stored.
    qpf_store_path : str
        Destination path to copy the files.
    start_time : datetime
        Start time of requested data.
    end_time : datetime
        End time of requested data.
    xmin, xmax, ymin, ymax : float
        Spatial domain for download_GFS.
    """

    # Ensure qpf_store_path exists
    download_folder = os.path.join(qpf_store_path, "gfs_data/")
    os.makedirs(download_folder, exist_ok=True)

    for f in glob.glob(os.path.join(download_folder, "*.tif")):
                os.remove(f)

    # Build list of expected times (hourly steps assumed)
    expected_times = []
    current = start_time
    
    while current <= end_time:
        expected_times.append(current)
        current += timedelta(hours=1)

    # Build expected file names
    expected_files = [
        os.path.join(path_gfs, f"gfs.{t:%Y%m%d%H%M}.tif") for t in expected_times
    ]
    
    missing_files = [f for f in expected_files if not os.path.exists(f)]
    if not missing_files:
        print("All files available. Copying to destination...")
        #copy files
        for f in expected_files:
            dest = os.path.join(download_folder, os.path.basename(f))
            try:
                shutil.copy2(f, dest)
            except Exception as e:
                print(f"Failed to copy {f}: {e}")
        print("Copy completed.")
    else:
        print(f"⚠️ Missing {len(missing_files)} files. Triggering download...")

    # Adjust start_time to previous GFS cycle (00,06,12,18)
        new_start = start_time.replace(minute=0, second=0, microsecond=0)
        while new_start.hour % 6 != 0:
            new_start -= timedelta(hours=1)
        # Call downloader
        download_GFS(new_start, end_time, xmin, xmax, ymin, ymax, download_folder)
        
        