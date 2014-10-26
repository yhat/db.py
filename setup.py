from distutils.core import setup
from setuptools import find_packages


required = [
    "prettytable==0.7.2",
    "pandas==0.14.1"
]

setup(
    name="db.py",
    version="0.1.6",
    author="Greg Lamp",
    author_email="greg@yhathq.com",
    url="https://github.com/yhat/db.py",
    license="BSD",
    packages=find_packages(),
    package_dir={"db": "db"},
    package_data={"db": []},
    description="a db package that doesn't suck",
    long_description=open("README.rst").read(),
    install_requires=required,
)

