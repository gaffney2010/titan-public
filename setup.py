from setuptools import setup

setup(
    name="titanpublic",
    description="Helper to read titan DB",
    author="T.J. Gaffney",
    packages=["titanpublic"],
    version="1.19.14",
    install_requires=[
        "attrs==21.4.0",
        "frozendict==2.3.4",
        "mysqlclient==2.1.1",
        "pandas==1.4.2",
        "pika==1.3.0",
        "pyyaml==6.0",
        "redis",
        "retrying==1.3.3"
    ],
)
