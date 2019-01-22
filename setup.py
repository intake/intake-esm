#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
import versioneer
from os.path import exists

readme = open("README.rst").read() if exists("README.rst") else ""



setup(
    name='intake-cesm',
    description='Intake-cesm provides a plug for reading CESM Large Ensemble data sets using intake',
    long_description=readme,
    maintainer='Anderson Banihirwe',
    maintainer_email='abanihi@ucar.edu',
    url='https://github.com/NCAR/intake-cesm',
    packages=find_packages(),
    package_dir={'intake-cesm': 'intake-cesm'},
    include_package_data=True,
    install_requires=[
    ],
    license='Apache 2.0',
    zip_safe=False,
    keywords='intake-cesm',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)