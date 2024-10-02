from setuptools import setup, find_packages

setup(
    name="dask-ec2-launcher",
    version="0.1",
    description="A decorator for launching Dask on AWS EC2 instances",
    author="Sudhindra Desai",
    author_email="helmoftitans@gmail.com",
    url="https://github.com/helmoftitans/cluster-service",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "fabric",
        "dask",
        "distributed",
        "s3fs"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)