from setuptools import setup, find_packages

setup(
    name="dask-ec2-launcher",
    version="0.1",
    description="A decorator for launching Dask on AWS EC2 instances",
    author="Sudhindra Desai",
    author_email="helmoftitans@gmail.com",
    url="https://github.com/helmoftitans/cluster-service",
    packages=find_packages(),  # Automatically find packages in the project
    install_requires=[
        "boto3>=1.26.0",  # AWS SDK
        "fabric>=2.7.0",  # SSH and automation tool
        "dask[distributed]~=2023.1",  # Dask for distributed computing
        "s3fs>=2023.1",  # S3 integration
        "decorator==4.4.2",  # Pin decorator version to 4.4.2 for moviepy compatibility
        "jedi>=0.16",  # Include jedi for ipython compatibility
        "moviepy==1.0.3",  # Ensure moviepy works with decorator 4.4.2
        "ipython>=7.34.0",  # Specify the version of ipython that works with jedi
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',  # Python version requirement
)