import sys
import os
import subprocess
import tempfile
from pathlib import Path
import fnmatch


TMP_DIR = tempfile.mkdtemp()

def _generate_cesmle_files(root_dir, tmp_dir=TMP_DIR):
    tmp_file_path = str(Path(tmp_dir)/"cesmle_files.txt")
    print(f"Creating temporary file: {tmp_file_path}")
    pattern = "*.nc"
    filelist = []

    with open(tmp_file_path, 'w') as outfile:
        for dir_name, sub_dir_name, flist in os.walk(root_dir):
            for filename in flist:
                if fnmatch.fnmatch(filename, pattern):
                    print(os.path.join(dir_name, filename), file=outfile)




def create_cesm_database(root_dir=None, db_path=None):
    if not os.path.exists(root_dir):
        raise NotADirectoryError(f"{root_dir} does not exist")
    _generate_cesmle_files(root_dir)

if __name__ == "__main__":
    create_cesm_database(root_dir="/glade/p_old/cesmLE/CESM-CAM5-BGC-LE/")