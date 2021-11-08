from setuptools import find_packages, setup

setup(
    name='excel2sqlserver',
    packages=find_packages(include=['excel2sqlserver']),
    version='0.1.0',
    description='Excel to sql server converter',
    author='Felipe Tufaile',
    license='MIT',
    install_requires=['pyodbc>=4.0.32', 
                      'pandas>=1.3.4', 
                      'regex>=2021.11.2', 
                      'tqdm>=4.62.3', 
                      'numpy>=1.21.4']
)

setup(
    name='postgresql2sqlserver',
    packages=find_packages(include=['excel2sqlserver']),
    version='0.1.0',
    description='Excel to sql server converter',
    author='Felipe Tufaile',
    license='MIT',
    install_requires=['pyodbc>=4.0.32', 
                      'pandas>=1.3.4', 
                      'regex>=2021.11.2', 
                      'tqdm>=4.62.3', 
                      'numpy>=1.21.4', 
                      'pgdumplib>=3.1.0', 
                      'python-math>=0.0.1']
)