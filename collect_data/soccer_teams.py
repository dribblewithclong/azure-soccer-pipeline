import configparser
import requests
import time
import datetime
import polars as pl
from notebookutils import mssparkutils

CONFIG_FILE = 'abfss://minedatalake@minestoragev2.dfs.core.windows.net/raw_data/config.ini'
OUTPUT_DIR = 'abfss://minedatalake@minestoragev2.dfs.core.windows.net/raw_data/teams'


class SoccerTeamsCollecting:

    def __init__(self, season):
        config = configparser.ConfigParser()
        config.read_string(
            mssparkutils.fs.head(CONFIG_FILE)
        )

        self.season = season
        self.api = eval(
            config.get('soccer_api', 'api')
        )
        self.competitions_id = eval(
            config.get('soccer_competitions', 'competitions_id')
        )

    def get_teams(self, competition_id):
        url = f'https://api.football-data.org/v4/competitions/{competition_id}/teams?season={self.season}'
        headers = {'X-Auth-Token': self.api}

        response = requests.get(
            url,
            headers=headers,
        )

        # Extract specific columns from data
        df = pl.DataFrame(response.json()['teams'])[
            [
                'id',
                'name',
                'shortName',
                'tla',
                'venue',
            ]
        ]

        # Rename column names
        df = df.rename(
            {
                'id': 'team_id',
                'name': 'team_name',
                'shortName': 'team_short_name',
                'tla': 'team_tla',
                'venue': 'team_stadium',
            }
        )

        return df

    def get_teams_top6_competitions(self):
        teams_list = list()

        for competition_id in self.competitions_id:
            teams_list.append(self.get_teams(competition_id))
            time.sleep(5)

        df = pl.concat(teams_list).unique().sort('team_id')

        # Export data
        df.to_pandas().to_csv(
            OUTPUT_DIR + f'/soccer_teams_season_{self.season}.csv',
            index=False,
        )

        return df


if __name__ == '__main__':
    season = datetime.datetime.now().year

    teams_collector = SoccerTeamsCollecting(season=season)
    teams_collector.get_teams_top6_competitions()
