from setuptools import setup, find_packages

setup(
  name='lanfang',
  version='0.1.3',
  description='Make daily work and learning more efficient',
  author='Donald Cheung',
  author_email='jianzhang9102@gmail.com',
  packages=find_packages(include=["lanfang*"]),
  url="https://github.com/zero91/lanfang",
  python_requires='>=3.5',
  install_requires=[
    "requests>=2.20.0",
  ],
  classifiers=[
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
    "Operating System :: OS Independent",
  ],
)
