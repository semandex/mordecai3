import pytest

from ..geoparse import Geoparser


@pytest.fixture(scope='session', autouse=True)
def geo():
    return Geoparser(event_geoparse=True, os_host='localhost', os_port=8502)
