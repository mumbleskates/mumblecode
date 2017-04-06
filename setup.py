# coding=utf-8
from setuptools import setup

setup(
    name="mumblecode",
    description="code and tools",
    version=0.1,
    author="Kent Ross",
    author_email="root.main@gmail.com",
    license="MIT",
    packages=[
        "mumblecode",
        "mumblecode.api",
    ],
    package_dir={"": "src"},
    install_requires=[
        'chardet',
        'future',
        'html5lib',
        'intervaltree',
        'lockfile',
        'pysha3',
        'requests',
        'tqdm',
    ],
    extras_require={
        'test': ['pytest-cov', 'tox']
    },
)
