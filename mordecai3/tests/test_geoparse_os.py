import pytest
from mordecai3.geoparse_os import Geoparser_OS
from mordecai3.elastic_utilities import get_client
from geojson_pydantic import Polygon

manila_buffer = {
    "type": "Polygon",
    "coordinates": [
        [
            [122, 15], # North-East
            [122, 13.5], # South-East
            [120., 13.5], # South-West
            [120., 15], # North-West
            [122, 15]  # Close the loop (same as first point)
        ]
    ]
}
geojson = Polygon(**manila_buffer)

@pytest.fixture(scope='session')
def geo_os():
    return Geoparser_OS(os_client=get_client())


def test_os_basic_location(geo_os):
    text = "The earthquake struck in the city of Christchurch, New Zealand."
    out = geo_os.geoparse_doc(text)
    assert out['geolocated_ents']
    assert any(ent['search_name'] == 'Christchurch' for ent in out['geolocated_ents'])
    assert any(ent['search_name'] == 'New Zealand' for ent in out['geolocated_ents'])

def test_os_no_location(geo_os):
    text = "There was a peaceful resolution to the conflict."
    out = geo_os.geoparse_doc(text)
    assert out['geolocated_ents'] == []

def test_os_country_filter(geo_os):
    text = "The capital of France is Paris."
    out = geo_os.geoparse_doc(text, include_countries=['FRA'])
    assert any(ent['search_name'] == 'Paris' for ent in out['geolocated_ents'])
    out_exclude = geo_os.geoparse_doc(text, exclude_countries=['FRA'])
    print("geolocated_ents with exclude_countries=['FRA']:", out_exclude['geolocated_ents'])
    assert all(ent['country_code3'] != 'FRA' for ent in out_exclude['geolocated_ents'])

def test_os_fuzzy_match(geo_os):
    text = "The city of Christchurh, New Zealand, was affected."
    out = geo_os.geoparse_doc(text, os_fuzziness=1)
    assert any('Christchurch' in ent['name'] for ent in out['geolocated_ents'])

def test_os_multiple_locations(geo_os):
    text = "London and Paris are major cities in Europe."
    out = geo_os.geoparse_doc(text)
    found = [ent['search_name'] for ent in out['geolocated_ents']]
    assert 'London' in found
    assert 'Paris' in found


def test_os_ambiguous_place(geo_os):
    text = "Springfield is a common city name in the United States."
    out = geo_os.geoparse_doc(text, include_countries=['USA'])
    assert any(ent['search_name'] == 'Springfield' and ent['country_code3'] == 'USA' for ent in out['geolocated_ents'])


def test_os_unmatched_entity(geo_os):
    text = "The city of Qwertyuiop does not exist."
    out = geo_os.geoparse_doc(text)
    assert any(ent['search_name'] == 'Qwertyuiop' for ent in out['unmatched_entities'])


def test_os_include_and_exclude(geo_os):
    text = "Berlin is in Germany, but not in France."
    out = geo_os.geoparse_doc(text, include_countries=['DEU', 'FRA'], exclude_countries=['FRA'])
    assert any(ent['search_name'] == 'Berlin' and ent['country_code3'] == 'DEU' for ent in out['geolocated_ents'])
    assert all(ent['country_code3'] != 'FRA' for ent in out['geolocated_ents'])


def test_os_case_insensitivity(geo_os):
    text = "paris is beautiful in the spring."
    out = geo_os.geoparse_doc(text)
    assert any(ent['search_name'].lower() == 'paris' for ent in out['geolocated_ents'])


def test_os_special_characters(geo_os):
    text = "São Paulo is the largest city in Brazil."
    out = geo_os.geoparse_doc(text)
    assert any('Sao Paulo' in ent['name'] or 'São Paulo' in ent['name'] for ent in out['geolocated_ents'])


def test_os_empty_string(geo_os):
    text = ""
    out = geo_os.geoparse_doc(text)
    assert out['geolocated_ents'] == []
    assert out['all_entities'] == []


def test_os_non_string_input(geo_os):
    with pytest.raises(ValueError):
        geo_os.geoparse_doc(12345)

def test_os_geojson_polygon_inclusion(geo_os):
    """
    Test that Geoparser_OS.geoparse_doc correctly includes locations inside a geojson polygon.
    """
    text = "Manila is a major city in the Philippines. Cebu is another city."
    out = geo_os.geoparse_doc(text, geojson=geojson)

    # Manila should be inside the polygon
    assert any(ent['search_name'] == 'Manila' for ent in out['geolocated_ents'])
    # any coordinates should be within the rectangular bounds
    assert any(ent['lon'] >= 120.0 and ent['lon'] <= 122.0 for ent in out['geolocated_ents'])
    assert any(ent['lat'] >= 13.5 and ent['lat'] <= 15.0 for ent in out['geolocated_ents'])

def test_os_geojson_polygon_empty(geo_os):
    """
    Test that an empty polygon returns no matches.
    """
    empty_poly = Polygon(type="Polygon", coordinates=[
        [
            [122, 15], # North-East
            [122, 15], # South-East
            [122, 15], # South-West
            [122, 15], # North-West
            [122, 15]  # Close the loop (same as first point)
        ]
    ])
    text = "Manila is a major city in the Philippines."
    out = geo_os.geoparse_doc(text, geojson=empty_poly)
    assert out['geolocated_ents'] == []

def test_os_geojson_polygon_country_filters(geo_os):
    """
    Test that include_countries=['PHL'] does not restrict Manila (since polygon already restricts),
    and exclude_countries=['PHL'] excludes Manila even if inside the polygon.
    """

    text = "Manila is a major city in the Philippines."
    # include_countries should not restrict Manila
    out_inc = geo_os.geoparse_doc(text, geojson=geojson, include_countries=['PHL'])
    assert any(ent['search_name'] == 'Manila' for ent in out_inc['geolocated_ents']), "Manila should be found with include_countries=['PHL'] and polygon."
    out_exc = geo_os.geoparse_doc(text, geojson=geojson, exclude_countries=['PHL'])
    assert out_exc['geolocated_ents'] == [], "Polygon should contain no places where exclude_countries=['PHL']."