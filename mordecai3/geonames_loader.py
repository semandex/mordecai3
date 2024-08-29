import logging
import sys
import csv
import os
import time
from datetime import datetime
from tqdm import tqdm
from opensearchpy import OpenSearch, helpers
from textacy.preprocessing.remove import accents as remove_accents

from mordecai3.elastic_utilities import GEO_INDEX_NAME, OPENSEARCH_PORT, OPENSEARCH_HOST

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def iso_convert(iso2c):
    """
    Takes a two character ISO country code and returns the corresponding 3
    character ISO country code.
    Parameters
    ----------
    iso2c: A two character ISO country code.
    Returns
    -------
    iso3c: A three character ISO country code.
    """

    iso_dict = {"AD": "AND", "AE": "ARE", "AF": "AFG", "AG": "ATG", "AI": "AIA",
                "AL": "ALB", "AM": "ARM", "AO": "AGO", "AQ": "ATA", "AR": "ARG",
                "AS": "ASM", "AT": "AUT", "AU": "AUS", "AW": "ABW", "AX": "ALA",
                "AZ": "AZE", "BA": "BIH", "BB": "BRB", "BD": "BGD", "BE": "BEL",
                "BF": "BFA", "BG": "BGR", "BH": "BHR", "BI": "BDI", "BJ": "BEN",
                "BL": "BLM", "BM": "BMU", "BN": "BRN", "BO": "BOL", "BQ": "BES",
                "BR": "BRA", "BS": "BHS", "BT": "BTN", "BV": "BVT", "BW": "BWA",
                "BY": "BLR", "BZ": "BLZ", "CA": "CAN", "CC": "CCK", "CD": "COD",
                "CF": "CAF", "CG": "COG", "CH": "CHE", "CI": "CIV", "CK": "COK",
                "CL": "CHL", "CM": "CMR", "CN": "CHN", "CO": "COL", "CR": "CRI",
                "CU": "CUB", "CV": "CPV", "CW": "CUW", "CX": "CXR", "CY": "CYP",
                "CZ": "CZE", "DE": "DEU", "DJ": "DJI", "DK": "DNK", "DM": "DMA",
                "DO": "DOM", "DZ": "DZA", "EC": "ECU", "EE": "EST", "EG": "EGY",
                "EH": "ESH", "ER": "ERI", "ES": "ESP", "ET": "ETH", "FI": "FIN",
                "FJ": "FJI", "FK": "FLK", "FM": "FSM", "FO": "FRO", "FR": "FRA",
                "GA": "GAB", "GB": "GBR", "GD": "GRD", "GE": "GEO", "GF": "GUF",
                "GG": "GGY", "GH": "GHA", "GI": "GIB", "GL": "GRL", "GM": "GMB",
                "GN": "GIN", "GP": "GLP", "GQ": "GNQ", "GR": "GRC", "GS": "SGS",
                "GT": "GTM", "GU": "GUM", "GW": "GNB", "GY": "GUY", "HK": "HKG",
                "HM": "HMD", "HN": "HND", "HR": "HRV", "HT": "HTI", "HU": "HUN",
                "ID": "IDN", "IE": "IRL", "IL": "ISR", "IM": "IMN", "IN": "IND",
                "IO": "IOT", "IQ": "IRQ", "IR": "IRN", "IS": "ISL", "IT": "ITA",
                "JE": "JEY", "JM": "JAM", "JO": "JOR", "JP": "JPN", "KE": "KEN",
                "KG": "KGZ", "KH": "KHM", "KI": "KIR", "KM": "COM", "KN": "KNA",
                "KP": "PRK", "KR": "KOR", "XK": "XKX", "KW": "KWT", "KY": "CYM",
                "KZ": "KAZ", "LA": "LAO", "LB": "LBN", "LC": "LCA", "LI": "LIE",
                "LK": "LKA", "LR": "LBR", "LS": "LSO", "LT": "LTU", "LU": "LUX",
                "LV": "LVA", "LY": "LBY", "MA": "MAR", "MC": "MCO", "MD": "MDA",
                "ME": "MNE", "MF": "MAF", "MG": "MDG", "MH": "MHL", "MK": "MKD",
                "ML": "MLI", "MM": "MMR", "MN": "MNG", "MO": "MAC", "MP": "MNP",
                "MQ": "MTQ", "MR": "MRT", "MS": "MSR", "MT": "MLT", "MU": "MUS",
                "MV": "MDV", "MW": "MWI", "MX": "MEX", "MY": "MYS", "MZ": "MOZ",
                "NA": "NAM", "NC": "NCL", "NE": "NER", "NF": "NFK", "NG": "NGA",
                "NI": "NIC", "NL": "NLD", "NO": "NOR", "NP": "NPL", "NR": "NRU",
                "NU": "NIU", "NZ": "NZL", "OM": "OMN", "PA": "PAN", "PE": "PER",
                "PF": "PYF", "PG": "PNG", "PH": "PHL", "PK": "PAK", "PL": "POL",
                "PM": "SPM", "PN": "PCN", "PR": "PRI", "PS": "PSE", "PT": "PRT",
                "PW": "PLW", "PY": "PRY", "QA": "QAT", "RE": "REU", "RO": "ROU",
                "RS": "SRB", "RU": "RUS", "RW": "RWA", "SA": "SAU", "SB": "SLB",
                "SC": "SYC", "SD": "SDN", "SS": "SSD", "SE": "SWE", "SG": "SGP",
                "SH": "SHN", "SI": "SVN", "SJ": "SJM", "SK": "SVK", "SL": "SLE",
                "SM": "SMR", "SN": "SEN", "SO": "SOM", "SR": "SUR", "ST": "STP",
                "SV": "SLV", "SX": "SXM", "SY": "SYR", "SZ": "SWZ", "TC": "TCA",
                "TD": "TCD", "TF": "ATF", "TG": "TGO", "TH": "THA", "TJ": "TJK",
                "TK": "TKL", "TL": "TLS", "TM": "TKM", "TN": "TUN", "TO": "TON",
                "TR": "TUR", "TT": "TTO", "TV": "TUV", "TW": "TWN", "TZ": "TZA",
                "UA": "UKR", "UG": "UGA", "UM": "UMI", "US": "USA", "UY": "URY",
                "UZ": "UZB", "VA": "VAT", "VC": "VCT", "VE": "VEN", "VG": "VGB",
                "VI": "VIR", "VN": "VNM", "VU": "VUT", "WF": "WLF", "WS": "WSM",
                "YE": "YEM", "YT": "MYT", "ZA": "ZAF", "ZM": "ZMB", "ZW": "ZWE",
                "CS": "SCG", "AN": "ANT", "YU": "YUG"}

    try:
        iso3c = iso_dict[iso2c]
        return iso3c
    except KeyError:
        logger.error(f"Bad code: {iso2c}")
        iso3c = "NA"
        return iso3c


def read_admin_codes(file):
    local_disc = {}
    with open(file, 'rt', encoding='utf-8') as adminCodesFile:
        reader = csv.reader(adminCodesFile, delimiter='\t')
        for row in reader:
            local_disc[row[0]] = row[1]
    return local_disc


class GeoNamesLoader:
    """
    Load the geo names from flat files into the Opensearch index with given name.
    It requires 3 params
    1. index_name - The name of the index in open search where data needs to be loaded
    2. os_client - Open search client with active connection
    3. data_dir - Directory were 3 text files should be present
        - admin1CodesASCII.txt
        - admin2Codes.txt
        - allCountries.txt
    """

    def __init__(self, index_name: str, os_client: OpenSearch, data_dir: str):
        self.index_name = index_name
        self.os_client = os_client
        self.data_dir = data_dir
        self.data_check = True
        if not os.path.exists(self.data_dir):
            logger.error(f"{data_dir} does not exists, nothing will get loaded")
            self.data_check = False

        self.adm1_file = f"{self.data_dir}/admin1CodesASCII.txt"
        if not os.path.exists(self.adm1_file):
            logger.error(f"{self.adm1_file} does not exists, Need this file to load geo names")
            self.data_check = False

        self.adm2_file = f"{self.data_dir}/admin2Codes.txt"
        if not os.path.exists(self.adm2_file):
            logger.error(f"{self.adm2_file} does not exists, Need this file to load geo names")
            self.data_check = False

        self.geocode_file = f'{self.data_dir}/allCountries.txt'
        if not os.path.exists(self.geocode_file):
            logger.error(f"{self.geocode_file} does not exists, Need this file to load geo names")
            self.data_check = False

    def documents(self, reader, adm1_dict, adm2_dict, expand_ascii=True):
        """
        Load Geonames entries provided by the `reader` into Opensearch.

        If `expand_ascii` = True, any alternative names with accents will have an
        accent-stripped form included in the alternative names list along with the original form.
        For example, the name "Ḩadīqat ash Shahbā" would generate a second entry
        "Hadiqat ash Shahba". This process does not affect non-Roman characters
        (e.g. "北京" remains "北京".)

        Parameters
        ----------
        reader: a CSV reader object
        adm1_dict: dict
          map from numeric ADM1 codes to names
        adm2_dict: dict
          map from numeric ADM2 codes to names
        expand_ascii: bool
          Include versions of names with accents stripped out.
        """
        today_date = datetime.today().strftime("%Y-%m-%d")
        error_count = 0
        good_count = 0
        for row in tqdm(reader, total=13040801):  # approx
            try:
                coords = row[4] + "," + row[5]
                country_code3 = iso_convert(row[8])
                alt_names = list(set(row[3].split(",")))
                # so annoying...add "US" as an alt name for USA
                if str(row[0]) == "6252001":
                    alt_names.append("US")
                    alt_names.append("U.S.")
                if str(row[0]) == "239880":
                    alt_names.append("C.A.R.")
                alt_name_length = len(alt_names)
                if expand_ascii:
                    stripped = [remove_accents(i) for i in alt_names]
                    both = alt_names + stripped
                    both = list(set(both))
                    alt_names = both
                # get ADM1 name
                if row[10]:
                    country_admin1 = '.'.join([row[8], row[10]])
                    try:
                        admin1_name = adm1_dict[country_admin1]
                    except KeyError:
                        admin1_name = ""
                else:
                    admin1_name = ""
                # Get ADM2 name
                if row[11]:
                    country_admin2 = '.'.join([row[8], row[10], row[11]])
                    try:
                        admin2_name = adm2_dict[country_admin2]
                    except KeyError:
                        admin2_name = ""
                        error_count += 1
                else:
                    admin2_name = ""
                doc = {"geonameid": row[0],
                       "name": row[1],
                       "asciiname": row[2],
                       "alternativenames": alt_names,
                       "coordinates": coords,  # 4, 5
                       "feature_class": row[6],
                       "feature_code": row[7],
                       "country_code3": country_code3,
                       "admin1_code": row[10],
                       "admin1_name": admin1_name,
                       "admin2_code": row[11],
                       "admin2_name": admin2_name,
                       "admin3_code": row[12],
                       "admin4_code": row[13],
                       "population": row[14],
                       "alt_name_length": alt_name_length,
                       "modification_date": today_date
                       }
                action = {"_index": "geonames",
                          "_id": doc['geonameid'],
                          "_source": doc}
                good_count += 1
                yield action
            except Exception as error:
                print(error, row)
                error_count += 1

        logger.info('Good entry count:', good_count)
        logger.info('Exception count:', error_count)

    def create_index_with_mapping(self):
        if self.os_client.indices.exists(index=self.index_name):
            logger.info(f"Index with name {self.index_name} already exits, nothing needed at this time")
            return

        os_mapping = """
            {
                "settings" : {
                    "number_of_shards" : 1,
                    "number_of_replicas" : 1
                },
                "mappings" : {
                    "properties" : {
                      "geonameid" : {"type" : "keyword", "index": "true"},
                      "name" : {"type" : "text"},
                      "asciiname" : {"type" : "text"},
                      "alternativenames" : {"type" : "text", "similarity" : "boolean",
                                            "norms": false},
                      "coordinates" : {"type" : "geo_point"},
                      "feature_class" :  {"type" : "keyword", "index":  "true"},
                      "feature_code" : {"type" : "keyword", "index":    "true"},
                      "country_code3" : {"type" : "keyword", "index":   "true"},
                      "admin1_code" : {"type" : "keyword", "index":  "true"},
                      "admin1_name" : {"type" : "keyword", "index":  "true"},
                      "admin2_code" : {"type" : "keyword", "index":  "true"},
                      "admin2_name" : {"type" : "keyword", "index":  "true"},
                      "admin3_code" : {"type" : "keyword", "index":  "true"},
                      "admin4_code" : {"type" : "keyword", "index":  "true"},
                      "population" : {"type" :  "long"},
                      "alt_name_length": {"type": "long"},
                      "modification_date" : {"type" : "date", "format": "date"}
                      }
                   }
            }
            """

        logger.info("loading mapping as ", os_mapping)
        self.os_client.indices.create(index=self.index_name, body=os_mapping)

    def data_check(self)-> bool:
        return self.data_check

    def load_geocodes(self):

        if not self.data_check:
            logger.error(
                f"Data check failed. Please make sure all required data file present in {self.data_dir} directory")
            return

        self.create_index_with_mapping()

        adm1_dict = read_admin_codes(self.adm1_file)
        # logger.info("Got admin1 dict as " , adm1_dict)

        adm2_dict = read_admin_codes(self.adm2_file)
        # logger.info("Got admin2 dict as " , adm2_dict)

        geocode_file = open(self.geocode_file, 'rt', encoding='utf-8')
        csv_reader = csv.reader(geocode_file, delimiter='\t')
        actions = self.documents(csv_reader, adm1_dict, adm2_dict)
        helpers.bulk(self.os_client, actions, chunk_size=1000)
        self.os_client.indices.refresh(index=self.index_name)


if __name__ == "__main__":
    try:
        geo_dir = os.environ["geo_names_data_dir"]
    except KeyError:
        logger.error("Please provide env variable 'geo_names_data_dir' where the geonames data is located")
        sys.exit(-1)

    index = GEO_INDEX_NAME
    host = OPENSEARCH_HOST
    port = OPENSEARCH_PORT
    client = OpenSearch(hosts=[{'host': host, 'port': port}])

    t = time.time()

    csv.field_size_limit()

    loader = GeoNamesLoader(index_name=index, os_client=client, data_dir=geo_dir)
    loader.load_geocodes()

    e = (time.time() - t) / 60
    logger.info("Total tile loading in minutes: ", e)
