from setuptools import setup

setup(
    name="titanpublic",
    description="Helper to read titan DB",
    author="T.J. Gaffney",
    packages=["titanpublic"],
    version="1.3.6",
    install_requires=[
        "attrs==21.4.0",
        "mysqlclient==2.1.1",
        "pandas==1.4.2",
    ],
)
