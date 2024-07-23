import os, subprocess
from pyarrow import fs

from src.utils import *


def connect_hdfs(hdfs_info):
    user = hdfs_info["user"]
    host = hdfs_info["host"]
    port = hdfs_info["port"]
    
    try:
        classpath = subprocess.Popen([hdfs_info["hdfs_path"], "classpath", "--glob"], stdout=subprocess.PIPE).communicate()[0]
        os.environ["CLASSPATH"] = classpath.decode("utf-8")
        hdfs = fs.HadoopFileSystem(host=hdfs_info["host"], port=hdfs_info["port"], user=hdfs_info["user"])
        
        return hdfs
    except Exception as e:
        #print(f"Failed to connect hdfs {user}@{host}:{port}")
        log(f"Failed to connect hdfs {user}@{host}:{port}", 1)
        return None

def find_files(start_prefix, end_prefix, local_dir):
    return [filename for filename in os.listdir(local_dir) if filename.startswith(start_prefix) and filename.endswith(end_prefix)]

def upload_file_to_hdfs(local_file_path, hdfs_path, hdfs):
    try:
        with open(local_file_path, 'rb') as local_file:
            with hdfs.open_output_stream(hdfs_path) as hdfs_file:
                hdfs_file.write(local_file.read())
    except Exception as e:
        log(f"Failed upload file {local_file_path}: {e}", 1)

def upload_csv_files_to_hdfs(start_prefix, local_dir, hdfs_dir, hdfs):
    try:
        if hdfs is None:
            raise ValueError("HDFS object is None, cannot upload file.")
        # Get all CSV files in the local directory
        for filename in find_files(start_prefix, ".csv", local_dir):
            if filename.startswith(start_prefix) and filename.endswith(".csv"):
                local_file_path = os.path.join(local_dir, filename)
                hdfs_file_path = os.path.join(hdfs_dir, filename)
                upload_file_to_hdfs(local_file_path, hdfs_file_path, hdfs)
                #print(f"Uploaded {local_file_path} to {hdfs_file_path}")
    except ValueError as ve:
        print(f"Failed upload csv files in local directory {local_dir} to hdfs {hdfs_dir}: {ve}")
        log(f"Failed upload csv files in local directory {local_dir} to hdfs {hdfs_dir}: {ve}", 1)
    except Exception as e:
        log(f"Failed upload csv files in local directory {local_dir} to hdfs {hdfs_dir}: {e}", 1)

def remove_files(start_prefix, end_prefix, local_dir):
    try:
        for filename in find_files(start_prefix, end_prefix, local_dir):
            file_path = os.path.join(local_dir, filename)
            os.remove(file_path)
    except Exception as e:
        log(f"Failed removing files in directory {file_path}: {e}", 1)