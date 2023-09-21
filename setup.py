from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
    'hydra-core==1.3.2',
    'statsd-exporter===3.2.1',
    # 'grpcio==1.58.0'
]

setup (
    name="dhuoflow",
    version="0.0.5",
    author="DHuO Data",
    author_email="antonio.lima@engdb.com.br",
    url="https://www.engdb.com.br/",
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(include=['src', 'src.*']),
    entry_points={
        'console_scripts': ['dhuoflow=src.main:main']
    }	
)