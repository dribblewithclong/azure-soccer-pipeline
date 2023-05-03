import configparser
import requests
import polars as pl
from notebookutils import mssparkutils

CONFIG_FILE = 'abfss://minedatalake@minestoragev2.dfs.core.windows.net/raw_data/config.ini'
OUTPUT_DIR = 'abfss://minedatalake@minestoragev2.dfs.core.windows.net/raw_data/areas'


class SoccerAreasCollecting:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read_string(
            mssparkutils.fs.head(CONFIG_FILE)
        )

        self.api = eval(
            config.get('soccer_api', 'api')
        )

    def get_areas(self):
        url = 'https://api.football-data.org/v4/areas/'
        headers = {'X-Auth-Token': self.api}

        response = requests.get(
            url,
            headers=headers,
        )

        # Extract specific columns from data
        df = pl.DataFrame(response.json()['areas'])[
            [
                'id',
                'name',
                'countryCode',
                'parentArea',
            ]
        ]

        # Rename column names
        df = df.rename(
            {
                'id': 'area_id',
                'name': 'area_name',
                'countryCode': 'area_country_code',
                'parentArea': 'area_parent'
            }
        )

        # Export data
        df.to_pandas().to_csv(
            OUTPUT_DIR + '/soccer_areas.csv',
            index=False,
        )

        return df


if __name__ == '__main__':
    area_collector = SoccerAreasCollecting()
    area_collector.get_areas()
