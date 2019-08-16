import os

from . import aggregate, config
from .bld_collection_utils import _extract_attr_with_regex, _reverse_filename_format
from .collection import Collection, docstrings
from .source import BaseSource


class CMIP5Collection(Collection):

    __doc__ = docstrings.with_indents(
        """ Builds a collection for CMIP5 data holdings.
    %(Collection.parameters)s
    """
    )

    def _get_file_attrs(self, filepath):
        """ Extract attributes of a file using information from CMIP5 DRS.

        Notes
        -----
        Reference:

        - CMIP5 DRS: https://pcmdi.llnl.gov/mips/cmip5/docs/cmip5_data_reference_syntax.pdf?id=27

        Directory:
          <activity>/
            <product>/
                <institute>/
                    <model>/
                        <experiment>/
                            <frequency>/
                                <modeling realm>/
                                    <MIP table>/
                                        <ensemble member>/
                                            <version number>/
                                                <variable name>/
                                                    <CMOR filename>.nc
        CMOR filename:
        <variable name>_<MIP table>_<model>_<experiment>_ <ensemble member>[_<temporal subset>][_<geographical info>].nc
        """
        keys = list(set(self.columns) - set(['resource', 'resource_type', 'direct_access']))
        fileparts = {key: None for key in keys}

        file_basename = os.path.basename(filepath)
        fileparts['file_basename'] = file_basename
        fileparts['file_fullpath'] = filepath

        filename_template = (
            '{variable}_{mip_table}_{model}_{experiment}_{ensemble_member}_{temporal_subset}.nc'
        )
        gridspec_template = '{variable}_{mip_table}_{model}_{experiment}_{ensemble_member}.nc'
        f = _reverse_filename_format(
            file_basename, filename_template=filename_template, gridspec_template=gridspec_template
        )
        fileparts.update(f)

        parent = os.path.dirname(filepath).strip('/')
        parent_split = parent.split(f"/{fileparts['model']}/")
        part_1 = parent_split[0].strip('/').split('/')
        part_2 = parent_split[1].strip('/').split('/')

        fileparts['file_dirname'] = os.path.dirname(filepath) + '/'
        fileparts['institute'] = part_1[-1]
        fileparts['frequency'] = part_2[1]
        fileparts['modeling_realm'] = part_2[2]

        # Sort in reverse order for the regex to work
        products = sorted(config.get('collections.cmip5.products'), reverse=True)
        version_regex = r'v\d{4}\d{2}\d{2}|v\d{1}'
        product_regex = r'|'.join(products)
        product = _extract_attr_with_regex(parent, regex=product_regex) or 'unknown'
        version = _extract_attr_with_regex(parent, regex=version_regex) or 'v0'
        fileparts['version'] = version
        fileparts['activity'] = config.get('collections.cmip5.mip_era')
        fileparts['product'] = product

        return fileparts


class CMIP5Source(BaseSource):
    name = 'cmip5'
    partition_access = True

    def _open_dataset(self):
        dataset_fields = ['institute', 'model', 'experiment', 'frequency', 'modeling_realm']
        self._open_dataset_groups(
            dataset_fields=dataset_fields,
            member_column_name='ensemble_member',
            variable_column_name='variable',
            file_fullpath_column_name='file_fullpath',
        )


class CMIP6Collection(Collection):

    __doc__ = docstrings.with_indents(
        """ Builds a collection for CMIP6 data holdings.
    %(Collection.parameters)s
    """
    )

    def _get_file_attrs(self, filepath):
        """ Extract attributes of a file using information from CMI6 DRS.

        Notes
        -----
        References
         1. CMIP6 DRS: http://goo.gl/v1drZl
         2. Controlled Vocabularies (CVs) for use in CMIP6:
            https://github.com/WCRP-CMIP/CMIP6_CVs

        Directory structure =

        <mip_era>/
            <activity_id>/
                <institution_id>/
                    <source_id>/
                        <experiment_id>/
                            <member_id>/
                                <table_id>/
                                    <variable_id>/
                                        <grid_label>/
                                            <version>

        file name =
        <variable_id>_<table_id>_<source_id>_<experiment_id >_<member_id>_<grid_label>[_<time_range>].nc

        For time-invariant fields, the last segment (time_range) above is omitted.

        Example when there is no sub-experiment: tas_Amon_GFDL-CM4_historical_r1i1p1f1_gn_196001-199912.nc
        Example with a sub-experiment:   pr_day_CNRM-CM6-1_dcppA-hindcast_s1960-r2i1p1f1_gn_198001-198412.nc


        """
        keys = list(set(self.columns) - set(['resource', 'resource_type', 'direct_access']))
        fileparts = {key: None for key in keys}

        file_basename = os.path.basename(filepath)
        fileparts['file_basename'] = file_basename
        fileparts['file_fullpath'] = filepath

        filename_template = '{variable_id}_{table_id}_{source_id}_{experiment_id}_{member_id}_{grid_label}_{time_range}.nc'
        gridspec_template = (
            '{variable_id}_{table_id}_{source_id}_{experiment_id}_{member_id}_{grid_label}.nc'
        )

        f = _reverse_filename_format(
            file_basename, filename_template=filename_template, gridspec_template=gridspec_template
        )
        fileparts.update(f)

        parent = os.path.dirname(filepath).strip('/')
        parent_split = parent.split(f"/{fileparts['source_id']}/")
        part_1 = parent_split[0].strip('/').split('/')

        grid_label = parent.split(f"/{fileparts['variable_id']}/")[1].strip('/').split('/')[0]

        fileparts['file_dirname'] = os.path.dirname(filepath) + '/'
        fileparts['grid_label'] = grid_label
        fileparts['activity_id'] = part_1[-2]
        fileparts['institution_id'] = part_1[-1]

        version_regex = r'v\d{4}\d{2}\d{2}|v\d{1}'
        version = _extract_attr_with_regex(parent, regex=version_regex) or 'v0'
        fileparts['version'] = version
        fileparts['mip_era'] = config.get('collections.cmip6.mip_era')
        return fileparts


class CMIP6Source(BaseSource):
    name = 'cmip6'
    partition_access = True

    def _open_dataset(self):
        # fields which define a single dataset
        dataset_fields = ['institution_id', 'source_id', 'experiment_id', 'table_id', 'grid_label']
        self._open_dataset_groups(
            dataset_fields=dataset_fields,
            member_column_name='member_id',
            variable_column_name='variable_id',
            file_fullpath_column_name='file_fullpath',
        )
