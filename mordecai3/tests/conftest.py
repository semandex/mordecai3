import pytest

from ..elastic_utilities import OPENSEARCH_HOST, OPENSEARCH_PORT
from ..geoparse import Geoparser


@pytest.fixture(scope='session', autouse=True)
def geo():
    return Geoparser(event_geoparse=True, os_host=OPENSEARCH_HOST, os_port=OPENSEARCH_PORT)
