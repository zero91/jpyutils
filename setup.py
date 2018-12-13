from setuptools import setup, find_packages

setup(
    name='jpyutils',
    version='0.1.0',
    description='To make daily work and study more efficient',
    author='Donald Cheung',
    author_email='jianzhang9102@gmail.com',
    packages=find_packages(include=["jpyutils*"]),
    python_requires='>=3.6',
)
