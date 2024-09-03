import pytest

from ..elastic_utilities import get_client
from ..geoparse import Geoparser


@pytest.fixture(scope='session', autouse=True)
def geo():
    return Geoparser(event_geoparse=True, os_client=get_client())
