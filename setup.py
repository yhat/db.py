from distutils.core import setup
from setuptools import find_packages


required = [
    "prettytable==0.7.2",
    "pandas==0.15.0",
    "boto==2.30.0"
]

setup(
    name="db.py",
    version="0.2.4",
    author="Greg Lamp",
    author_email="greg@yhathq.com",
    url="https://github.com/yhat/db.py",
    license="BSD",
    packages=find_packages(),
    package_dir={"db": "db"},
    package_data={"db": ["data/*.sqlite"]},
    description="a db package that doesn't suck",
    long_description=open("README.rst").read(),
    install_requires=required,
)
