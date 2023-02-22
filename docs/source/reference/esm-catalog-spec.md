# ESM Catalog Specification

```{note}
This documents mirrors the [ESM Collection Specification](https://github.com/NCAR/esm-collection-spec/blob/master/collection-spec/collection-spec.md) and is updated as the specification evolves.
```

## Overview

This document explains the structure and content of an ESM Catalog.
A catalog provides metadata about the catalog, telling us what we expect to find inside and how to open it.
The catalog is described is a single json file, inspired by the STAC spec.

The ESM Catalog specification consists of three parts:

### Catalog Specification

The _catalog_ specification provides metadata about the catalog, telling us what we expect to find inside and how to open it.
The descriptor is a single json file, inspired by the [STAC spec](https://github.com/radiantearth/stac-spec).

```json
{
  "esmcat_version": "0.1.0",
  "id": "sample",
  "description": "This is a very basic sample ESM catalog.",
  "catalog_file": "sample_catalog.csv",
  "attributes": [
    {
      "column_name": "activity_id",
      "vocabulary": "https://raw.githubusercontent.com/WCRP-CMIP/CMIP6_CVs/master/CMIP6_activity_id.json"
    },
    {
      "column_name": "source_id",
      "vocabulary": "https://raw.githubusercontent.com/WCRP-CMIP/CMIP6_CVs/master/CMIP6_source_id.json"
    }
  ],
  "assets": {
    "column_name": "path",
    "format": "zarr"
  }
}
```

### Catalog

The collection points to a single catalog.
A catalog is a CSV file.
The meaning of the columns in the csv file is defined by the parent collection.

```
activity_id,source_id,path
CMIP,ACCESS-CM2,gs://pangeo-data/store1.zarr
CMIP,GISS-E2-1-G,gs://pangeo-data/store1.zarr
```

### Assets (Data Files)

The data assets can be either netCDF or Zarr.
They should be either [URIs](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier) or full filesystem paths.

## Catalog fields

| Element             | Type                                                      | Description                                                                                                                                                            |
| ------------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| esmcat_version      | string                                                    | **REQUIRED.** The ESM Catalog version the catalog implements.                                                                                                          |
| id                  | string                                                    | **REQUIRED.** Identifier for the catalog.                                                                                                                              |
| title               | string                                                    | A short descriptive one-line title for the catalog.                                                                                                                    |
| description         | string                                                    | **REQUIRED.** Detailed multi-line description to fully explain the catalog. [CommonMark 0.28](http://commonmark.org/) syntax MAY be used for rich text representation. |
| catalog_file        | string                                                    | **REQUIRED.** Path to a the CSV file with the catalog contents.                                                                                                        |
| catalog_dict        | array                                                     | If specified, it is mutually exclusive with `catalog_file`. An array of dictionaries that represents the data that would otherwise be in the csv.                      |
| attributes          | [[Attribute Object](#attribute-object)]                   | **REQUIRED.** A list of attribute columns in the data set.                                                                                                             |
| assets              | [Assets Object](#assets-object)                           | **REQUIRED.** Description of how the assets (data files) are referenced in the CSV catalog file.                                                                       |
| aggregation_control | [Aggregation Control Object](#aggregation-control-object) | **OPTIONAL.** Description of how to support aggregation of multiple assets into a single xarray data set.                                                              |

### Attribute Object

An attribute object describes a column in the catalog CSV file.
The column names can optionally be associated with a controlled vocabulary, such as the [CMIP6 CVs](https://github.com/WCRP-CMIP/CMIP6_CVs), which explain how to interpret the attribute values.

| Element     | Type   | Description                                                                            |
| ----------- | ------ | -------------------------------------------------------------------------------------- |
| column_name | string | **REQUIRED.** The name of the attribute column. Must be in the header of the CSV file. |
| vocabulary  | string | Link to the controlled vocabulary for the attribute in the format of a URL.            |

### Assets Object

An assets object describes the columns in the CSV file relevant for opening the actual data files.

| Element            | Type   | Description                                                                                                                                                                                                            |
| ------------------ | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| column_name        | string | **REQUIRED.** The name of the column containing the path to the asset. Must be in the header of the CSV file.                                                                                                          |
| format             | string | The data format. Valid values are `netcdf`, `zarr`, `opendap` or `reference` ([`kerchunk`](https://github.com/fsspec/kerchunk) reference files). If specified, it means that all data in the catalog is the same type. |
| format_column_name | string | The column name which contains the data format, allowing for variable data types in one catalog. Mutually exclusive with `format`.                                                                                     |

### Aggregation Control Object

An aggregation control object defines neccessary information to use when aggregating multiple assets into a single xarray data set.

| Element              | Type                                        | Description                                                                             |
| -------------------- | ------------------------------------------- | --------------------------------------------------------------------------------------- |
| variable_column_name | string                                      | **REQUIRED.** Name of the attribute column in csv file that contains the variable name. |
| groupby_attrs        | array                                       | Column names (attributes) that define data sets that can be aggegrated.                 |
| aggregations         | [[Aggregation Object](#aggregation-object)] | **OPTIONAL.** List of aggregations to apply to query results                            |

### Aggregation Object

An aggregation object describes types of operations done during the aggregation of multiple assets into a single xarray data set.

| Element        | Type   | Description                                                                                                                                                                                                                                                                                                                                                                                          |
| -------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| type           | string | **REQUIRED.** Type of aggregation operation to apply. Valid values include: `join_new`, `join_existing`, `union`                                                                                                                                                                                                                                                                                     |
| attribute_name | string | Name of attribute (column) across which to aggregate.                                                                                                                                                                                                                                                                                                                                                |
| options        | object | **OPTIONAL.** Aggregration settings that are passed as keywords arguments to [`xarray.concat()`](https://xarray.pydata.org/en/stable/generated/xarray.concat.html) or [`xarray.merge()`](https://xarray.pydata.org/en/stable/generated/xarray.merge.html#xarray.merge). For `join_existing`, it must contain the name of the existing dimension to use (for e.g.: something like `{'dim': 'time'}`). |
