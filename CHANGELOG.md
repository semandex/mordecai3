## Mordecai3 Changes

### 3.0.0a.8
- Fix the issue with invalid start and end position being returned due to cleaning of the input text [#24]

### 3.0.0a.7
- Fix the issue with text containing special characters failing in extracting locations [#19]

### 3.0.0a.6
- Fix the issue with text containing `\n` failing in getting the location after the upgrades

### 3.0.0a.5
- Remove the dependency on opensearch-dsl and upgrade opensearch-py to 3.0.0
- Also upgrade torch min version to 2.7.0

### 3.0.0a.4
- Fix the issue with some perfect match being dropped ( https://github.com/semandex/mordecai3/issues/13 )

### 3.0.0a.3
- Add support for filtering location results by list of countries

### 3.0.0a.2
- Change GeoParse constructor to take opensearch client as an argument