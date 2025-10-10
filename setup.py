
from setuptools import setup, find_packages

setup(
    name='iptu-recife-project',
    version='0.1.0',
    # Assume que todo o código de produção está dentro de 'src'
    packages=find_packages(where='src'), 
    package_dir={'': 'src'},
)