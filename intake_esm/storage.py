import fnmatch
import os
import shutil
from subprocess import PIPE, Popen
from warnings import warn


class StorageResource(object):
    """ Defines a storage resource object"""

    def __init__(self, urlpath, loc_type, exclude_patterns, file_extension='.nc'):
        """

        Parameters
        -----------

        urlpath : str
              Path to storage resource
        loc_type : str
              Type of storage resource. Supported resources include: posix, hsi (tape)
        exclude_patterns : str, list
               Directories to exclude during catalog generation
        file_extension : str, default `.nc`
              File extension

        """

        self.urlpath = urlpath
        self.type = loc_type
        self.file_extension = file_extension
        self.exclude_patterns = exclude_patterns
        self.filelist = self._list_files()

    def _list_files(self):
        """ Get file listing for different location types such as
            tapes, posix filesystem, filelist.
        """

        if self.type == 'posix':
            filelist = self._list_files_posix()

        elif self.type == 'hsi':
            filelist = self._list_files_hsi()

        elif self.type == 'input-file':
            filelist = self._list_files_input_file()

        else:
            raise ValueError(f'unknown resource type: {self.type}')

        return filter(self._filter_func, filelist)

    def _filter_func(self, path):
        return not any(
            fnmatch.fnmatch(path, pat=exclude_pattern) for exclude_pattern in self.exclude_patterns
        )

    def _list_files_posix(self):
        """Get a list of files"""
        try:
            w = os.walk(self.urlpath)

            filelist = []

            for root, dirs, files in w:
                filelist.extend(
                    [os.path.join(root, f) for f in files if f.endswith(self.file_extension)]
                )

            return filelist
        except Exception as e:
            warn(
                f'{e.__str__()}\nCould not parse content in directory = {self.urlpath}. Skipping directory.'
            )
            return []

    def _list_files_hsi(self):
        """Get a list of files from HPSS tapes"""
        if shutil.which('hsi') is None:
            print(f'no hsi; cannot access [HSI]{self.urlpath}')
            return []

        p = Popen(
            [
                'hsi',
                'find {urlpath} -name "*{file_extension}"'.format(
                    urlpath=self.urlpath, file_extension=self.file_extension
                ),
            ],
            stdout=PIPE,
            stderr=PIPE,
        )

        stdout, stderr = p.communicate()
        lines = stderr.decode('UTF-8').strip().split('\n')[1:]

        filelist = []
        i = 0
        while i < len(lines):
            if '***' in lines[i]:
                i += 2
                continue
            else:
                filelist.append(lines[i])
                i += 1

        return filelist

    def _list_files_input_file(self):
        """return a list of files from a file containing a list of files"""
        with open(self.urlpath, 'r') as fid:
            return fid.read().splitlines()
