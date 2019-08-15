""" Implementation for The ECMWF ERA5 Reanalyses data holdings """
import os

from tqdm.auto import tqdm

from . import aggregate, config
from .bld_collection_utils import _ensure_file_access, _reverse_filename_format, get_subset
from .collection import Collection, docstrings
from .source import BaseSource


class CORDEXCollection(Collection):

    __doc__ = docstrings.with_indents(
        """ Builds a NA-CORDEX collection for data
        stored on NCAR's GLADE
    %(Collection.parameters)s
    """
    )

    def _get_file_attrs(self, filepath):
        file_basename = os.path.basename(filepath)
        keys = list(set(self.columns) - set(['resource', 'resource_type', 'direct_access']))

        fileparts = {key: None for key in keys}
        fileparts['file_basename'] = file_basename
        fileparts['file_fullpath'] = filepath
        fileparts['file_dirname'] = os.path.dirname(filepath) + '/'
        filename_template = '{variable}.{experiment}.{global_climate_model}.{regional_climate_model}.{frequency}.{grid}.{bias_corrected_or_raw}.nc'

        f = _reverse_filename_format(file_basename, filename_template)
        fileparts.update(f)

        return fileparts


class CORDEXSource(BaseSource):
    name = 'cordex'
    partition_access = True

    def _open_dataset(self):
        # fields which define a single dataset
        dataset_fields = [
            'global_climate_model',
            'regional_climate_model',
            'frequency',
            'grid',
            'bias_corrected_or_raw',
            'experiment',
        ]

        kwargs = self._validate_kwargs(self.kwargs)

        all_dsets = {}
        ds = get_subset(self.collection_name, self.query)

        file_fullpath_column_name = 'file_fullpath'
        file_basename_column_name = 'file_basename'
        variable_column_name = 'variable'

        df = _ensure_file_access(ds, file_fullpath_column_name, file_basename_column_name)
        grouped = df.groupby(dataset_fields)
        for dset_keys, dset_files in tqdm(
            grouped, desc='dataset', disable=not config.get('progress-bar')
        ):
            dset_id = '.'.join(dset_keys)
            var_dsets = []
            for v_id, v_files in dset_files.groupby(variable_column_name):
                urlpath_ei_vi = v_files[file_fullpath_column_name].tolist()
                dsets = [
                    aggregate.open_dataset_delayed(
                        url,
                        data_vars=[v_id],
                        chunks=kwargs['chunks'],
                        decode_times=kwargs['decode_times'],
                    )
                    for url in urlpath_ei_vi
                ]

                var_dset_i = aggregate.concat_time_levels(
                    dsets,
                    time_coord_name_default=kwargs['time_coord_name'],
                    override_coords=kwargs['override_coords'],
                )
                var_dsets.append(var_dset_i)

            _dset_i = aggregate.merge(dsets=var_dsets)
            all_dsets[dset_id] = _dset_i

        self._ds = all_dsets
