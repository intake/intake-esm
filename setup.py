#!/usr/bin/env python

"""The setup script."""

from os.path import exists

from setuptools import find_packages, setup

with open('requirements.txt') as f:
    install_requires = f.read().strip().split('\n')


if exists('README.md'):
    with open('README.md') as f:
        long_description = f.read()
else:
    long_description = ''

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Intended Audience :: Science/Research',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Topic :: Scientific/Engineering',
]

setup(
    name='intake-esm',
    description='An intake plugin for parsing an ESM (Earth System Model) Collection/catalog and loading assets (netCDF files and/or Zarr stores) into xarray datasets.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    python_requires='>=3.7',
    maintainer='NCAR XDev Team',
    maintainer_email='xdev@ucar.edu',
    classifiers=CLASSIFIERS,
    url='https://intake-esm.readthedocs.io',
    project_urls={
        'Documentation': 'https://intake-esm.readthedocs.io',
        'Source': 'https://github.com/intake/intake-esm',
        'Tracker': 'https://github.com/intake/intake-esm/issues',
    },
    packages=find_packages(exclude=('tests',)),
    package_dir={'intake-esm': 'intake-esm'},
    include_package_data=True,
    install_requires=install_requires,
    license='Apache 2.0',
    zip_safe=False,
    entry_points={
        'intake.drivers': [
            'esm_datastore = intake_esm.core:esm_datastore',
            'esm_group = intake_esm.source:ESMGroupDataSource',
            'esm_single_source = intake_esm.source:ESMDataSource',
        ]
    },
    keywords='intake, xarray, catalog',
    use_scm_version={'version_scheme': 'post-release', 'local_scheme': 'dirty-tag'},
)
