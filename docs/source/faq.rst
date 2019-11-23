==========================
Frequently Asked Questions
==========================

How do I create my own catalog?
-------------------------------
Intake-esm catalogs include two pieces: 1) a ESM-Collection file and
2) a Catalog file. 

1. The ESM-Collection file is a simple json file that provides metadata about
the catalog. The specification for this json file is found in the
`esm-collection-spec <https://github.com/NCAR/esm-collection-spec/blob/master/collection-spec/collection-spec.md>`_
repository. 

2. The Catalog file is a CSV file that lists the catalog contents. This file
includes one row per dataset granule (e.g. a NetCDF file or Zarr dataset).
The columns in this CSV must match the attributes and assets listed in the
ESM-Collection file. A short example of a Catlog file is shown below::

    activity_id,institution_id,source_id,experiment_id,member_id,table_id,variable_id,grid_label,zstore,dcpp_init_year
    AerChemMIP,BCC,BCC-ESM1,piClim-CH4,r1i1p1f1,Amon,ch4,gn,gs://cmip6/AerChemMIP/BCC/BCC-ESM1/piClim-CH4/r1i1p1f1/Amon/ch4/gn/,
    AerChemMIP,BCC,BCC-ESM1,piClim-CH4,r1i1p1f1,Amon,clt,gn,gs://cmip6/AerChemMIP/BCC/BCC-ESM1/piClim-CH4/r1i1p1f1/Amon/clt/gn/,
    AerChemMIP,BCC,BCC-ESM1,piClim-CH4,r1i1p1f1,Amon,co2,gn,gs://cmip6/AerChemMIP/BCC/BCC-ESM1/piClim-CH4/r1i1p1f1/Amon/co2/gn/,
    AerChemMIP,BCC,BCC-ESM1,piClim-CH4,r1i1p1f1,Amon,evspsbl,gn,gs://cmip6/AerChemMIP/BCC/BCC-ESM1/piClim-CH4/r1i1p1f1/Amon/evspsbl/gn/,
    ...

Is there a list of existing catalogs?
-------------------------------------

The table below is an incomplete list of existing catalogs.
Please feel free to add to this list.

+-------------------+--------------------+-----------------+
| Catalog Name      | Platform           | Details         |
+===================+====================+=================+
| CMIP6-GLADE       | NCAR-Cheyenne      | TBD             |
+-------------------+--------------------+-----------------+
| CMIP6-GCP         | GCP                | TBD             |
+-------------------+--------------------+-----------------+
| CMIP5-GLADE       | NCAR-Cheyenne      | TBD             |
+-------------------+--------------------+-----------------+
| CESM-GLADE        | NCAR-Cheyenne      | TBD             |
+-------------------+--------------------+-----------------+
| CESM LENS         | AWS                | TBD             |
+-------------------+--------------------+-----------------+

