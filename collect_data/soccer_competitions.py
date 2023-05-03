import configparser
import requests
import polars as pl
from notebookutils import mssparkutils

CONFIG_FILE = 'abfss://minedatalake@minestoragev2.dfs.core.windows.net/raw_data/config.ini'
OUTPUT_DIR = 'abfss://minedatalake@minestoragev2.dfs.core.windows.net/raw_data/competitions'


class SoccerCompetitionsCollecting:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read_string(
            mssparkutils.fs.head(CONFIG_FILE)
        )

        self.api = eval(
            config.get('soccer_api', 'api')
        )

    def get_competitions(self):
        url = 'https://api.football-data.org/v4/competitions/'
        headers = {'X-Auth-Token': self.api}

        response = requests.get(
            url,
            headers=headers,
        )

        # Extract specific columns from data
        df = pl.DataFrame(response.json()['competitions'])[
            [
                'id',
                'name',
                'code',
                'type',
            ]
        ]

        # Rename column names
        df = df.rename(
            {
                'id': 'competition_id',
                'name': 'competition_name',
                'code': 'competition_code',
                'type': 'competition_type'
            }
        )

        # Export data
        df.to_pandas().to_csv(
            OUTPUT_DIR + '/soccer_competitions.csv',
            index=False,
        )

        return df


if __name__ == '__main__':
    competitions_collector = SoccerCompetitionsCollecting()
    competitions_collector.get_competitions()
