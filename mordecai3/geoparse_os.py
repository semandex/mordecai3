import re

import numpy as np
import spacy
from opensearchpy import Q
import jellyfish

from mordecai3.elastic_utilities import get_adm1_country_entry, get_country_by_name
from mordecai3.geoparse import Geoparser, doc_to_ex_expanded


class Geoparser_OS(Geoparser):
    def geoparse_doc(self,
                     text,
                     include_countries: list[str] | None = None,
                     exclude_countries: list[str] | None = None,
                     os_match_threshold=1.0,
                     os_fuzziness=0,
                     **kwargs):
        """
        Geoparse a document.

        Parameters
        ----------
        text : str or spacy Doc (with ._.tensor attributes)
            The text to geoparse.
        include_countries : list[str]
            If provided, the geoparser will only consider locations in the given list of countries.
        exclude_countries : list[str]
            If provided, the geoparser will exclude locations in the given list of countries.
        os_match_threshold: float
            The minimum similarity threshold (0.0-1.0) for OpenSearch clause matching. Gets turned into a percentage
            and passed as OpenSearch's "minimum_should_match" parameter. Defaults to 1.0 (100%) for high precision.
        os_fuzziness: int
            OpenSearch fuzziness parameter. Defaults to 0 (only exact word matches after clean up). Setting higher
            values may increase recall for e.g., typos, but can affect precision.

        Returns
        -------
        output : dict
            Includes the following keys:
            - "doc_text": a string of the input text
            - "event_location_raw":
            - "all_entities": list of dicts, each dict contains information about all named entities found in the document, including:
                * text: the entity text
                * label: the NER label (GPE, LOC, PERSON, etc.)
                * start_char/end_char: character positions in the document
                * processed: boolean indicating if this entity type was processed for geoparsing
            - "unmatched_entities": list of dicts, each dict contains entities that could not be matched in OpenSearch, including:
                * search_name: the place name that was searched
                * start_char/end_char: character positions
                * reason: explanation of why no match was found
            - "geolocated_ents": list of dicts, each dict is the best geoparsed location for each processed entity

        Example
        -------
        >>> text = "The earthquake struck in the city of Christchurch, New Zealand."
        >>> geoparser.geoparse_doc(text)
        """

        if type(text) is str:
            doc = self.nlp(text)
        elif type(text) is spacy.tokens.doc.Doc:
            doc = text
        else:
            raise ValueError("Text must be either of type 'str' or 'spacy.tokens.doc.Doc'.")

        # build all_entities for output
        all_entities = []
        for ent in doc.ents:
            entity_info = {
                "text": ent.text,
                "label": ent.label_,
                "start_char": ent.start_char,
                "end_char": ent.end_char,
                "processed": ent.label_ in ['GPE', 'LOC', 'EVENT_LOC', 'FAC', 'ORG']  # this should use common set
            }
            all_entities.append(entity_info)

        # extract spacy entities
        doc_ex = doc_to_ex_expanded(doc)

        # get country filter for OpenSearch
        country_filter = get_country_filter(include_countries, exclude_countries)

        # Query OpenSearch for each entity
        unmatched_entities = []
        geolocated_ents = []
        for ex in doc_ex:
            search_name = ex['search_name']
            search_name = _clean_search_name(search_name)

            # This tries to guess relations like "Berlin, Germany", not sure if valuable
            if 'in_rel' in ex.keys():
                if ex['in_rel']:
                    parent_place = get_country_by_name(ex['in_rel'], self.conn)
                    if not parent_place:
                        parent_place = get_adm1_country_entry(ex['in_rel'], None, self.conn)
                else:
                    parent_place = None
            else:
                parent_place = None

            # Run the query
            q = {"multi_match": {"query": search_name,
                                 "fields": ['name', 'alternativenames', 'asciiname'],
                                 "fuzziness": os_fuzziness,
                                 "minimum_should_match": f"{int(os_match_threshold * 100)}%"
                                 }}
            res = self.conn.query(q)
            if country_filter:
                res = res.filter(country_filter)
            res = res[0:1].execute()

            # get results, add some distances, etc.
            choices = _os_res_formatter(res, search_name, parent_place)
            hits = res.hits.hits
            for c, h in zip(choices, hits):
                c['es_score'] = h['_score']
                # Use a constant dummy score for compatibility with downstream validation logic
                c['score'] = 1.0

            # Put future filtering here?

            if choices:
                best = choices[0]
                best["search_name"] = ex['search_name']
                best["start_char"] = ex['start_char']
                best["end_char"] = ex['end_char']
                ## Add in city info here
                best['city_id'], best['city_name'] = self.lookup_city(best)
                geolocated_ents.append(best)
            else:
                unmatched_entity = {
                    "search_name": ex['search_name'],
                    "start_char": ex['start_char'],
                    "end_char": ex['end_char'],
                    "reason": "No OpenSearch results found"
                }
                unmatched_entities.append(unmatched_entity)

        output = {"doc_text": doc.text,
                  "event_location_raw": '',
                  "all_entities": all_entities,
                  "unmatched_entities": unmatched_entities,
                  "geolocated_ents": geolocated_ents}

        return output


def get_country_filter(include_countries, exclude_countries):
    include_country_filter = None
    if include_countries:
        include_country_filter = Q("terms", country_code3=include_countries)
    exclude_country_filter = None
    if exclude_countries:
        exclude_country_filter = ~Q("terms", country_code3=exclude_countries)

    country_filter = None
    if include_country_filter:
        country_filter = include_country_filter

    if exclude_country_filter:
        if country_filter:
            country_filter &= exclude_country_filter
        else:
            country_filter = exclude_country_filter
    return country_filter

def _clean_search_name(search_name):
    """
    Strip out place names that might be preventing the right results
    """
    search_name = re.sub("^the", "", search_name, flags=re.IGNORECASE).strip()
    search_name = re.sub("tribal district", "", search_name).strip()
    search_name = re.sub("[Cc]ity", "", search_name).strip()
    search_name = re.sub("[Dd]istrict", "", search_name).strip()
    search_name = re.sub("[Mm]etropolis", "", search_name).strip()
    search_name = re.sub("[Cc]ounty", "", search_name).strip()
    search_name = re.sub("[Rr]egion", "", search_name).strip()
    search_name = re.sub("[Pp]rovince", "", search_name).strip()
    search_name = re.sub("[Tt]erritory", "", search_name).strip()
    search_name = re.sub("[Bb]ranch", "", search_name).strip()
    search_name = re.sub("'s$", "", search_name).strip()
    # super hacky!! This one is the most egregious
    if search_name == "US" or search_name == "U.S.": #added U.S. case
        search_name = "United States"
    return search_name

def _os_res_formatter(res, search_name, parent=None):
    """
    Helper function to format the ES/Geonames results into an output format.
    Based on mordecai3.elastic_utilities.res_formatter, but uses a different distance normalization
    Not sure if all this code is necessary, but keeping it for simplicity

    Parameters
    ----------
    res: Elasticsearch/Geonames output
    search_name: str
      The original search term from the document
    parent: dict
      Geonames/ES entry for the inferred parent

    Returns
    -------
    choices: list
      List of formatted Geonames results, including edit distance statistics
    """
    # choices is our eventual output, a list of dicts, each of which is a formatted Geonames result
    choices = []
    alt_lengths = []
    min_dist = []
    max_dist = []
    avg_dist = []
    ascii_dist = []
    # iterate through the docs returned by ES
    for i in res['hits']['hits']:
        i = i.to_dict()['_source']
        names = [i['name']] + i['alternativenames']
        dists = [jellyfish.levenshtein_distance(search_name, j) for j in names]
        lat, lon = i['coordinates'].split(",")
        d = {"feature_code": i['feature_code'],
            "feature_class": i['feature_class'],
            "country_code3": i['country_code3'],
            "lat": float(lat),
            "lon": float(lon),
            "name": i['name'],
            "admin1_code": i['admin1_code'],
            "admin1_name": i['admin1_name'],
            "admin2_code": i['admin2_code'],
            "admin2_name": i['admin2_name'],
            "geonameid": i['geonameid']}
        # if we detect a parent country or ADM1, add the parent match features
        if parent:
            if parent['admin1_name'] == "":
                d['admin1_parent_match'] = 0
            elif parent['admin1_name'] == i['admin1_name']:
                d['admin1_parent_match'] = 1
            else:
                d['admin1_parent_match'] = -1

            if parent['country_code3'] == "":
                d['country_code_parent_match'] = 0
            elif parent['country_code3'] == i['country_code3']:
                d['country_code_parent_match'] = 1
            else:
                d['country_code_parent_match'] = -1
        else:
            d['admin1_parent_match'] = 0
            d['country_code_parent_match'] = 0

        choices.append(d)
        alt_lengths.append(len(i['alternativenames'])+1)
        min_dist.append(np.min(dists))
        max_dist.append(np.max(dists))
        avg_dist.append(np.mean(dists))
        ascii_dist.append(jellyfish.levenshtein_distance(search_name, i['asciiname']))
    alt_lengths = np.log(alt_lengths)
    min_dist = np.array(min_dist) / len(search_name)  # More useful than old normalization
    max_dist = np.array(max_dist) / len(search_name)
    avg_dist = np.array(avg_dist) / len(search_name)
    ascii_dist = np.array(ascii_dist) / len(search_name)

    for n, i in enumerate(choices):
        i['alt_name_length'] = alt_lengths[n]
        i['min_dist'] = min_dist[n]
        i['max_dist'] = max_dist[n]
        i['avg_dist'] = avg_dist[n]
        i['ascii_dist'] = ascii_dist[n]
    return choices
