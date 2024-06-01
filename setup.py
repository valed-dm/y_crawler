from setuptools import setup, find_packages

# Read the requirements from the file
with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='hn_scraper',
    version='0.1',
    packages=find_packages(),
    install_requires=required,
    entry_points={
        'console_scripts': [
            'hn_scraper = hn_scraper.main:main',
        ],
    },
)
