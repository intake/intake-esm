import logging
import os
import re

import numpy as np
import pandas as pd
import xarray as xr
from dask import delayed
from tqdm.autonotebook import tqdm

from . import aggregate, config
from .common import BaseSource, Collection, StorageResource, get_subset

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class GMETCollection(Collection):
    def __init__(self, collection_spec):
        super(GMETCollection, self).__init__(collection_spec)
        self.root_dir = self.collection_spec['data_sources']['root_dir']['urlpath']

    def build(self):
        self._validate()

        for experiment, experiment_attrs in self.collection_spec['data_source'].items():
            logger.warning(f'Working on experiment: {experiment}')

    def assemble_file_list(self, experiment, experiment_attrs):
        df_files = {}
        for location in experiment_attrs['locations']:
            res_key = ':'.join([location['name'], location['loc_type'], location['urlpath']])
            if res_key not in df_files:
                logger.warning(f'Getting file listing : {res_key}')

                if 'exclude_dirs' not in location:
                    location['exclude_dirs'] = []

                resource = StorageResource(
                    urlpath=location['urlpath'],
                    loc_type=location['loc_type'],
                    exclude_dirs=location['exclude_dirs'],
                )

                df_files[res_key] = self._assemble_collection_df_files(
                    resource_key=res_key,
                    resource_type=location['loc_type'],
                    direct_access=location['direct_access'],
                    filelist=resource.filelist,
                )
