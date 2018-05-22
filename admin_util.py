"""Utilities for ak-energy-data administrative tasks.
"""
import os
import math
import numbers
from glob import glob
import boto3


# Maps file extensions to MIME Content-Type
content_type_map = {
    'html': 'text/html',
    'csv': 'text/csv',
    'txt': 'text/plain',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'pkl': 'application/octet-stream'
}

def content_type(file_name):
    """Returns a Content-Type for the file with the path 'file_name', based on the
    file extension.  Uses 'application/octet-stream' if extension is not in the
    content_type_map above.
    """
    ext = os.path.splitext(file_name)[-1].strip('.')
    return content_type_map.get(ext, 'application/octet-stream')

def clear_dir(dir_path):
    """Deletes all the files found in the 'dir_path' directory on the
    local machine, except for a file named '.gitignore'.  NOTE: glob
    naturally ignores files starting with a dot (.).
    """
    for fn in glob(os.path.join(dir_path, '*')):
        if os.path.isfile(fn):
            print(f'deleting {fn}')
            os.remove(fn)

def save_df(df, dest_path):
    """Saves locally a Pandas DataFrame, 'df', as both a bz2 compressed Pickle file
    and a CSV file.  The path to the saved file is 'dest_path', but the extension
    '.pkl' is added for the Pickle version, and the extension '.csv' is added for the
    CSV version.
    """
    print(f'saving DataFrame to {dest_path}.pkl and .csv')
    df.to_pickle(f'{dest_path}.pkl', compression='bz2')
    df.to_csv(f'{dest_path}.csv')

def chg_nonnum(val, sub_val):
    """Changes a nan or anything that is not a number to 'sub_val'.  
    Otherwise returns val.
    """
    if isinstance(val, numbers.Number):
        if math.isnan(val):
            return sub_val
        else:
            return val
    else:
        return sub_val


class Bucket:

    def __init__(self, bucket_name):
        """'bucket_name' is the name of the S3 bucket
        that will be operated on.
        """

        self.s3 = boto3.resource('s3')
        self.bucket_name = bucket_name
        self.bucket = self.s3.Bucket(bucket_name)

    def upload_file(self, file_path, dest_key):
        """Uploads one file to the bucket storing it at key 'dest_key'.  Assigns a
        content type from the extension on the file.
        """
        cnt_typ = content_type(file_path)
        data = open(file_path, 'rb')
        self.bucket.put_object(Key=dest_key, 
                 Body=data,
                 ContentType=cnt_typ,
                )
        print(f'uploaded {file_path} with Content Type: {cnt_typ}')

    def clear_dir(self, dir_to_clear):
        """Deletes all files in the directory 'dir_to_clear'."""
        dir_to_clear = dir_to_clear.strip('/')
        for obj in self.bucket.objects.filter(Prefix = dir_to_clear):
            print(f'deleting {obj.key}')
            obj.delete()

    def upload_dir(self, src_dir_local, dest_dir, clear_dest_dir=False):
        """Uploads an entire local directory, 'src_dir_local', to the bucket, storing in the 
        'dest_dir' in the bucket. If 'clear_dest_dir' is True, the destination directory 
        is cleared before uploading. Files starting with a dot (.) are not uploaded.
        """
        # remove any leading and trailing slashes from destination directory.
        dest_dir = dest_dir.strip('/')

        if clear_dest_dir:
            self.clear_dir(dest_dir)

        for fn in glob(os.path.join(src_dir_local, '*')):
            base_name = os.path.basename(fn)
            dest_key = f'{dest_dir}/{base_name}'
            self.upload_file(fn, dest_key)

    def move_files(self, src_dir, dest_dir, clear_dest=False):
        """Moves all of the files in the 'source_prefix' folder to the
        'dest_prefix' folder. If 'clear_dest' is True all files in the 'dest_prefix'
        are deleted first.
        """
        src_dir = src_dir.strip('/')
        dest_dir = dest_dir.strip('/')
        
        # delete items in 'dest_prefix' folder
        if clear_dest:
            self.clear_dir(dest_dir)

        # copy items from source and delete original after copy
        for obj in self.bucket.objects.filter(Prefix = src_dir):
            if obj.key.strip('/')==src_dir:
                continue
            base_name = os.path.basename(obj.key)
            src = f'{obj.bucket_name}/{obj.key}'
            print(f'copying {src}')
            self.s3.Object(self.bucket_name, f'{dest_dir}/{base_name}').copy_from(CopySource=src)
            obj.delete()
        