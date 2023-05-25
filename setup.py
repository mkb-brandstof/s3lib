"""Setup script for s3lib.
"""

from setuptools import setup, find_packages

setup(
    name='s3lib',
    version='0.1.0',
    packages=find_packages(),
    install_requires=['boto3']
)