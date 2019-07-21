# import intake_xarray
import xarray as xr
import pandas as pd
import os
from tqdm.autonotebook import tqdm

from intake.source.utils import reverse_format
from . import aggregate
from .collection import Collection, get_subset
from .source import BaseSource

class ACCESSCollection(Collection):

    """ Defines BLUELINK ACCESS dataset collection

    Parameters
    ----------
    collection_spec : dict

    """
    def _get_file_attrs(self, filepath, urlpath):

        path_fmt = urlpath + '{model_version}/{grid}/{level}/y{year:d}/m{month:d}/{forecast_date:%Y%m%d}/{rg:1s}{forecast_hour:d}.nc'
        try:
            fileparts = reverse_format(path_fmt, filepath)
        except ValueError:
            raise ValueError('Failed to parse ' + filepath)
            # print('Failed to parse ' + filepath)
            # return None
 
        fileparts['file_basename'] = os.path.basename(filepath)
        fileparts['file_dirname'] = os.path.dirname(filepath) + '/'
        fileparts['file_fullpath'] = filepath
        fileparts['product_type'] = 'forecast'

        return fileparts

from dask import compute, delayed

class ACCESSSource(BaseSource):
    name = 'access'
    partition_access = True

    def _open_dataset(self):
        """
        Notes
        -----
        - TBD

        """

        @delayed
        def _open_delayed(fn,data_vars=None,bbox=None):
            ds = xr.open_dataset(fn,chunks={})
            if data_vars is not None:
                ds = ds[data_vars]
            if bbox is not None:
                ds = ds.sel(lon=slice(bbox[0],bbox[1]),lat=slice(bbox[2],bbox[3]))
            return ds

        kwargs = self._validate_kwargs(self.kwargs)

        dataset_fields = ['model_version','grid']
        grouped = get_subset(self.collection_name, self.query).groupby(dataset_fields)

        all_dsets = {}
        for m_id, group_files in tqdm(grouped, desc='Loading model_versions and grids'):

            # Concatenate the hours for each forecast
            subgroups=group_files.groupby('forecast_date')
            forecast_dates=[]
            subgroup_ds=[]
            for forecast_date, m_files in tqdm(subgroups, "Loading " + str(m_id)):

                files = sorted(m_files['file_fullpath'].tolist())
                forecast_hour = m_files['forecast_hour'].to_list()

                futures = [_open_delayed(fn,data_vars=kwargs['data_vars'],bbox=kwargs['bbox']) for fn in files]
                hour_dsets = compute(futures)[0]
                forecast_dset = xr.concat(hour_dsets,dim='time')
                forecast_dset['forecast_hour']=('time',pd.to_timedelta(forecast_hour,'h'))
                forecast_dset=forecast_dset.swap_dims({'time':'forecast_hour'})

                forecast_dates.append(forecast_date)
                subgroup_ds.append(forecast_dset)
            
            # Stack the forecasts
            group_ds=xr.concat(subgroup_ds,dim=pd.DatetimeIndex(pd.to_datetime(forecast_dates),name='forecast_date'))
            group_ds = group_ds.stack(forecast_time=['forecast_date','forecast_hour'])
            group_ds = group_ds.swap_dims({'forecast_time':'time'})

            group_ds=group_ds.assign_coords(fc_cycle=('time',[ft[0] for ft in group_ds['forecast_time'].values]))
            group_ds=group_ds.assign_coords(fc_hour=('time',[ft[1] for ft in group_ds['forecast_time'].values]))
            group_ds=group_ds.drop('forecast_time')

            # These are forecast datasets
            all_dsets[m_id]=group_ds

        self._ds = all_dsets
