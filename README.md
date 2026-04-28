# Mordecai v3

Mordecai3 is a new geoparser that replaces the earlier [Mordecai](https://github.com/openeventdata/mordecai) geoparser. It uses spaCy to identify place names in text, retrieves candidate geolocations from the Geonames gazetteer running in a local Elasticsearch index, and ranks the candidate results using a neural model trained on around 6,000 gold standard training examples.

## Usage

```pycon
>>> from mordecai3 import Geoparser
>>> geo = Geoparser()
>>> geo.geoparse_doc("I visited Alexanderplatz in Berlin.")
{'doc_text': 'I visited Alexanderplatz in Berlin.',
 'event_location_raw': '',
 'geolocated_ents': [{'admin1_code': '16',
                      'admin1_name': 'Berlin',
                      'admin2_code': '00',
                      'admin2_name': '',
                      'city_id': '',
                      'city_name': '',
                      'country_code3': 'DEU',
                      'end_char': 24,
                      'feature_class': 'S',
                      'feature_code': 'SQR',
                      'geonameid': '6944049',
                      'lat': 52.5225,
                      'lon': 13.415,
                      'name': 'Alexanderplatz',
                      'score': 1.0,
                      'search_name': 'Alexanderplatz',
                      'start_char': 10},
                     {'admin1_code': '16',
                      'admin1_name': 'Berlin',
                      'admin2_code': '00',
                      'admin2_name': '',
                      'city_id': '2950159',
                      'city_name': 'Berlin',
                      'country_code3': 'DEU',
                      'end_char': 34,
                      'feature_class': 'P',
                      'feature_code': 'PPLC',
                      'geonameid': '2950159',
                      'lat': 52.52437,
                      'lon': 13.41053,
                      'name': 'Berlin',
                      'score': 1.0,
                      'search_name': 'Berlin',
                      'start_char': 28}]} 
```

## Installation and Requirements

To install Mordecai3, run

```bash
pip install mordecai3
```

The library has two external dependencies that you'll need to set up.

First, run following command to download the spaCy model used to identify place names and to compute the tensors used in the ranking model.

```bash
python -m spacy download en_core_web_trf
```

Second, Mordecai3 requires a local instance of Opensearch with a Geonames index. 

To build this index, you will need to download few files into a directory, for example "geo_names_data" directory. Here are the flat files to download
```shell
cd geo_names_data
curl https://download.geonames.org/export/dump/allCountries.zip -o allCountries.zip
curl https://download.geonames.org/export/dump/admin1CodesASCII.txt -o admin1CodesASCII.txt
curl https://download.geonames.org/export/dump/admin2Codes.txt -o admin2Codes.txt

unzip allCountries.zip
```

This should create 3 text files like these in `geo_name_data` directory
```shell
admin1CodesASCII.txt
admin2Codes.txt
allCountries.txt
```

Once you have this director with 3 text files, you can use `GeoNamesLoader` class to load into your opensearch instance. 
Here is a sample code to load it using GeoNamesLoader utility class
```python
    client = OpenSearch(hosts=[{'host': 'localhost', 'port': 9200}])
    loader = GeoNamesLoader(index_name='geonames', os_client=client, data_dir='geo_name_data')
    loader.load_geocodes()
```


If you're doing event geoparsing, that step requires other models to be downloaded from https://huggingface.co/. These will be automatically downloaded the first time the program is run (if it's 


## Details and Citation

More details on the model and its accuracy are available here: https://arxiv.org/abs/2303.13675

If you use Mordecai 3, please cite:

```bibtex
@article{halterman2023mordecai,
      title={Mordecai 3: A Neural Geoparser and Event Geocoder}, 
      author={Andrew Halterman},
      year={2023},
      journal={arXiv preprint arXiv:2303.13675}
}
```

## Acknowledgements

This work was sponsored by the Political Instability Task Force (PITF). The PITF is funded by the Central Intelligence Agency. The views expressed in this here are the authors' alone and do not represent the views of the US Government.

# Step to release this library 

- Change the version in `pyproject.toml`
- Run `pytest` to make sure all test passes

```bash
pip install build twine # if needed
python -m build
```

Or after running `setup.py sdist`
```shell
twine upload --repository nexus dist/*
```

## Geoparsing Observations / Recommendations
Spotchecking, there are problems with both Geoparser and Geoparser_OS. Some examples available [here](https://docs.google.com/spreadsheets/d/1iVoEUBDZ0qu4hbiQX_FXypOoUb3a7A_f5JBabJ5fizQ/edit?usp=sharing). Feel free to add more examples.

Relying on OpenSearch (Geoparser_OS) gives good looking text results, but tends to incorrectly pick small towns instead of intuitive large cities. Full Mordecai (Geoparser) excludes many obvious matches (e.g., "Angeles City") and picks mismatched text as well (Ormoc Port -> Legaspi Port).

Looking deeper into Mordecai, there are some strange choices in the neural model design:
- Padding logits should be -inf (probability zero) but seem to be set to a significant probability.
- The model adds the embeddings from the "other places" and sends them to the model. This seems pretty arbitrary and could lead to weird results since there's not much training data (~6000 samples).
- In general, considering the wide variety of place types and countries/languages, 6000 training samples seems quite small.

Here are some ideas for improving geoparsing:
- To the extent possible, use real world text chunks from as many countries as possible.
- Compare spacy and gliner extraction, possibly train on both.
- Use a flagship LLM (minimum LLAMA 70B or similar) to generate ground truth for ranking, 50-100k samples (more couldn't hurt). 
  - Define exactly what behavior we want
  - Add suitable prompt with some examples. 
  - Iterate as needed
- Fix padding issues.
- Include both cross entropy (correctness) and distance losses (give partial credit for proximity). 
  - Might need to normalize distances by place type or size. 
- Use a transformer / attention layer - this should help with redundant place entries that are close to each other. 
- Check how/what info from OS results are going into the model. 
- Consider fine tuning embeddings in some fashion. 
