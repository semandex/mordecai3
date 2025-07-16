# Step to release this library 
- Change the version in `setup.py` script
- Run `pytest` to make sure all test passes
```shell
python setup.py sdist upload -r nexus
```