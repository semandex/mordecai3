from setuptools import find_packages, setup

setup(
    name='mordecai3',
    version='3.0.0a.14',
    url='http://github.com/ahalterman/mordecai3/',
    author='Andy Halterman',
    author_email='ahalterman0@gmail.com',
    license='MIT',
    python_requires='>=3.10',
    keywords=['geoparsing', 'nlp', 'geocoding', 'toponym resolution'],
    packages=find_packages(),
    install_requires=[
        'typer>=0.16.0,<1.0',
        'spacy-transformers>=1.3.9,<2.0',
        'transformers>=4.49.0,<5.0',
        'spacy>=3.8.7,<4.0',
        'torch>=2.7.1,<3.0',
        'scikit-learn>=1.7.1',
        'pandas>=2.0.0',
        'jellyfish>=1.2.0,<2.0',
        'tqdm>=4.67.1,<5.0',
        'numpy>=2.0.0',
        'jsonlines>=3.0.0,<4.0',
        'xmltodict>=0.14.2,<1.0',
        'opensearch-py>=3.0.0'
    ],
    dependency_links=[
        'https://github.com/explosion/spacy-models/releases/tag/en_core_web_trf-3.8.0'
    ],
    include_package_data=True,
    package_data={'mordecai3': ['assets/admin1CodesASCII.json',
                                'assets/country_bert_768.npy',
                                'assets/countryInfo.txt',
                                'assets/feature_code_dict.json',
                                'assets/hierarchy.txt',
                                'assets/mordecai_2024-06-04.pt',
                                'assets/wikipedia-iso-country-codes.txt']}
)
