#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
import versioneer
from os.path import exists

readme = open("README.rst").read() if exists("README.rst") else ""

requirements = ["intake", "intake-xarray"]

setup(
    name='intake-cesmle',
    description='Intake-cesmle provides a plug for reading CESM Large Ensemble dataasets using intake',
    long_description=readme,
    maintainer='Anderson Banihirwe',
    maintainer_email='abanihi@ucar.edu',
    url='https://github.com/NCAR/intake-cesmle',
    packages=[
        'intake-cesmle',
    ],
    package_dir={'intake-cesmle': 'intake-cesmle'},
    include_package_data=True,
    install_requires=requirements,
    license='Apache-2.0 license',
    zip_safe=False,
    keywords='intake-cesmle',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)