#!/usr/bin/env python3
"""
Setup script for MyRepETL package
"""

from setuptools import setup, find_packages
import os

# Read the contents of README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Read requirements
with open(os.path.join(this_directory, 'requirements.txt'), encoding='utf-8') as f:
    requirements = []
    for line in f:
        line = line.strip()
        if line and not line.startswith('#'):
            # Remove version constraints for dependencies that might conflict
            if line.startswith('pytest') or line.startswith('black') or line.startswith('flake8') or line.startswith('mypy'):
                continue  # Skip dev dependencies
            requirements.append(line)

setup(
    name="myrepetl",
    version="1.0.0",
    author="Tumurzakov",
    author_email="tumurzakov@example.com",
    description="MySQL Replication ETL Tool - инструмент для репликации данных из MySQL с поддержкой трансформаций",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tumurzakov/myrepetl",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-asyncio>=0.21.1",
            "pytest-mock>=3.12.0",
            "pytest-cov>=4.1.0",
            "black>=23.11.0",
            "flake8>=6.1.0",
            "mypy>=1.7.1",
        ],
    },
    entry_points={
        "console_scripts": [
            "myrepetl=myrepetl.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.json", "*.sql", "*.yaml", "*.yml"],
    },
    keywords="mysql, replication, etl, binlog, data-pipeline, database",
    project_urls={
        "Bug Reports": "https://github.com/tumurzakov/myrepetl/issues",
        "Source": "https://github.com/tumurzakov/myrepetl",
        "Documentation": "https://github.com/tumurzakov/myrepetl#readme",
    },
)
