# setup.py
from setuptools import setup, find_packages

setup(
    name="data_analytics_platform",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "sqlalchemy",
        "pandas",
        "pydantic",
        "fastapi",
        "uvicorn",
        "python-dotenv",
        "keyring",
        "cryptography",
        "pyyaml",
    ],
)