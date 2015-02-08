from distutils.core import setup
from setuptools import find_packages


required = [
    "prettytable==0.7.2",
    "pandas",
    "pybars3==0.7.2"
]

setup(
    name="db.py",
    version="0.4.0",
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
    classifiers=[
        # Maturity
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',
        # License
        'License :: OSI Approved :: BSD License',
        # Versions supported
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],

)
