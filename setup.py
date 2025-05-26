from setuptools import setup, find_packages

setup(
    name="fuel_price_analysis",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pandas>=2.1.0',
        'kaggle>=1.5.16',
        'pyspark>=3.5.5',
        'pyarrow>=14.0.1',
        'python-dotenv>=1.0.0',
        'findspark>=2.0.1',
        'openpyxl>=3.1.2'  # Para leitura de arquivos Excel
    ],
    extras_require={
        'dev': [
            'jupyter>=1.0.0',
            'notebook>=7.0.6',
            'pytest>=7.4.0',
            'pytest-cov>=4.1.0',
            'black>=23.7.0',
            'flake8>=6.1.0',
            'isort>=5.12.0'
        ]
    },
    python_requires='>=3.8',
    author="Willian de Oliveira",
    description="Análise de preços de combustíveis no Brasil",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ]
)
