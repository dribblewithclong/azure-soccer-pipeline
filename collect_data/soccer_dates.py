import datetime
import polars as pl

OUTPUT_DIR = 'abfss://minedatalake@minestoragev2.dfs.core.windows.net/raw_data/dates'


class SoccerDatesCollecting:

    def __init__(self, season):
        self.season = season
        self.weekday_dict = {
            0: 'Monday',
            1: 'Tuesday',
            2: 'Wednesday',
            3: 'Thursday',
            4: 'Friday',
            5: 'Saturday',
            6: 'Sunday',
        }
        self.from_time = datetime.datetime(
            self.season,
            8,
            1,
        )
        self.to_time = datetime.datetime(
            self.season + 1,
            7,
            1,
        )

    def create_date_table(self):
        datetime_list = pl.date_range(
            self.from_time,
            self.to_time,
        ).to_list()

        df = pl.DataFrame(datetime_list, schema=['date_id'])

        df = df.with_columns(
            (
                df['date_id'].apply(lambda x: x.date())
            ).alias('date')
        )
        df = df.with_columns(
            (
                df['date_id'].apply(lambda x: x.weekday())
            ).alias('week_day')
        )
        df = df.with_columns(
            (
                df['date_id'].apply(
                    lambda x: '0' + str(x.day)
                    if x.day < 10
                    else str(x.day)
                )
            ).alias('day')
        )
        df = df.with_columns(
            (
                df['date_id'].apply(
                    lambda x: '0' + str(x.month)
                    if x.month < 10
                    else str(x.month)
                )
            ).alias('month')
        )
        df = df.with_columns(
            (
                df['date_id'].apply(lambda x: str(x.year))
            ).alias('year')
        )
        df = df.with_columns(
            (
                pl.concat_str(df['year'], df['month'], df['day'])
                .apply(lambda x: int(x))
            ).alias('date_id')
        )
        df = df.with_columns(
            (
                df['week_day'].map_dict(self.weekday_dict)
            ).alias('week_day')
        )

        # Export data
        df.to_pandas().to_csv(
            OUTPUT_DIR + f'/soccer_dates_season_{self.season}.csv',
            index=False,
        )

        return df


if __name__ == '__main__':
    season = datetime.datetime.now().year

    dates_collector = SoccerDatesCollecting(season=season)
    dates_collector.create_date_table()
