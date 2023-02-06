from setuptools import setup, find_packages

setup(
    name='oracle_to_hive',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pyspark',
    ],
    author='Your Name',
    author_email='your.email@example.com',
    description='A package that transfers data from Oracle to Hive using PySpark',
)
