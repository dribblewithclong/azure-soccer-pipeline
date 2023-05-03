import configparser
import requests
import time
import datetime
import polars as pl
from notebookutils import mssparkutils

CONFIG_FILE = 'abfss://minedatalake@minestoragev2.dfs.core.windows.net/raw_data/config.ini'
OUTPUT_DIR = 'abfss://minedatalake@minestoragev2.dfs.core.windows.net/raw_data/matches'


class SoccerMatchesCollecting:

    def __init__(self, from_time, to_time):
        config = configparser.ConfigParser()
        config.read_string(
            mssparkutils.fs.head(CONFIG_FILE)
        )

        self.from_time = from_time
        self.to_time = to_time
        self.api = eval(
            config.get('soccer_api', 'api')
        )
        self.competitions_id = eval(
            config.get('soccer_competitions', 'competitions_id')
        )

    def get_matches(self, competition_id):
        url_competitions = f'https://api.football-data.org/v4/competitions/{competition_id}'
        url_matches = f'/matches?dateFrom={self.from_time}&dateTo={self.to_time}'
        url = url_competitions + url_matches
        headers = {'X-Auth-Token': self.api}

        response = requests.get(
            url,
            headers=headers,
        )

        cols_to_get = [
            'id',
            'area',
            'competition',
            'utcDate',
            'homeTeam',
            'awayTeam',
            'score'
        ]

        # Check if there is no match in these days
        if len(response.json()['matches']) == 0:
            return False

        # Extract specific columns from data
        df = pl.DataFrame(response.json()['matches'])[cols_to_get]

        # Extract scores
        df = df.with_columns(
            (
                df['score'].apply(lambda x: x['fullTime']['home'])
            ).alias('home_score')
        )
        df = df.with_columns(
            (
                df['score'].apply(lambda x: x['fullTime']['away'])
            ).alias('away_score')
        )
        df = df.drop('score')

        # Rename column names
        df = df.rename(
            {
                'utcDate': 'utc_date',
                'homeTeam': 'home_team',
                'awayTeam': 'away_team',
            }
        )

        return df

    def get_matches_top6_competitions(self):
        matches_list = list()

        for competition_id in self.competitions_id:
            if self.get_matches(competition_id) is not False:
                matches_list.append(self.get_matches(competition_id))
                time.sleep(10)

        if len(matches_list) == 0:
            return None

        df = pl.concat(matches_list).unique('id').sort('utc_date')

        # Export data
        df.to_pandas().to_parquet(
            OUTPUT_DIR + f'/soccer_matches_from_{self.from_time}_to_{self.to_time}.parquet'
        )

        return df


if __name__ == '__main__':
    to_time = str(
        datetime.datetime.now().date() - datetime.timedelta(days=1)
    )
    from_time = str(
        datetime.datetime.now().date() - datetime.timedelta(days=7)
    )

    matches_collector = SoccerMatchesCollecting(
        from_time=from_time,
        to_time=to_time,
    )
    matches_collector.get_matches_top6_competitions()
