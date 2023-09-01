import pytest
import spacy

from ..geoparse import Geoparser


@pytest.fixture(scope='session', autouse=True)
def geo():
    return Geoparser()
