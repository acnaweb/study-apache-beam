from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
    'hydra-core==1.3.2'
]

setup (
    name="dhuoflow",
    version="0.0.5",
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(include=['src', 'src.*']),
    entry_points={
        'console_scripts': ['dhuoflow=src.main:main']
    }	
)