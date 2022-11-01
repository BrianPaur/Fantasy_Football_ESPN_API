from espn_api.football import League
import pandas as pd
import matplotlib.lines as lines
import numpy as np
from os.path import exists
from os import path
import sqlite3
import sqlalchemy as db
import statistics

pd.options.display.width = None
pd.options.display.max_columns = None
pd.set_option("display.max_rows", 3000)
pd.set_option("display.max_columns", 3000)
pd.set_option("display.float_format", lambda x: "%.5f" % x)


class FantasyFootball:
    def __init__(
        self,
        league_id=608256606,
        league_year=2021,
        league_length=15,
        dataFrame=None,
        player_count=None,
    ):
        self.league_id = league_id
        self.league_year = league_year
        self.league_length = league_length
        self.dataFrame = dataFrame
        self.player_count = player_count

    def number_of_weeks(self):
        pass

    def leagueInfo(self):
        league = League(self.league_id, self.league_year)
        week_current = league.current_week - 1

        Team_Name = []
        Wins = []
        Losses = []
        mov = []
        Score = []
        for i in league.teams:
            a = i.team_name
            b = i.wins
            c = i.losses
            d = i.mov
            s = i.scores

            Team_Name.append(a)
            Wins.append(b)
            Losses.append(c)
            mov.append(d)
            Score.append(s)

        df = pd.DataFrame(
            list(zip(Team_Name, Wins, Losses)), columns=["Team Name", "Wins", "Losses"]
        )

        # pulls scores into a df based on week
        df_2 = pd.DataFrame(Score)
        df_2 = df_2.iloc[:, : self.league_length]
        df_2.columns = [
            "Week 1",
            "Week 2",
            "Week 3",
            "Week 4",
            "Week 5",
            "Week 6",
            "Week 7",
            "Week 8",
            "Week 9",
            "Week 10",
            "Week 11",
            "Week 12",
            "Week 13",
            "Week 14",
            "Week 15",
        ]

        # sums the week totals
        sum_column = (
            df_2["Week 1"]
            + df_2["Week 2"]
            + df_2["Week 3"]
            + df_2["Week 4"]
            + df_2["Week 5"]
            + df_2["Week 6"]
            + df_2["Week 7"]
            + df_2["Week 8"]
            + df_2["Week 9"]
            + df_2["Week 10"]
            + df_2["Week 11"]
            + df_2["Week 12"]
            + df_2["Week 13"]
            + df_2["Week 14"]
            + df_2["Week 15"]
        )

        df_2["Points Scored"] = sum_column

        # margin of victory
        df_3 = pd.DataFrame(mov)
        df_3 = df_3.iloc[:, : self.league_length]
        df_3.columns = [
            "Week 1 MOV",
            "Week 2 MOV",
            "Week 3 MOV",
            "Week 4 MOV",
            "Week 5 MOV",
            "Week 6 MOV",
            "Week 7 MOV",
            "Week 8 MOV",
            "Week 9 MOV",
            "Week 10 MOV",
            "Week 11 MOV",
            "Week 12 MOV",
            "Week 13 MOV",
            "Week 14 MOV",
            "Week 15 MOV",
        ]

        # Average MOV
        ave_column = (
            df_3["Week 1 MOV"]
            + df_3["Week 2 MOV"]
            + df_3["Week 3 MOV"]
            + df_3["Week 4 MOV"]
            + df_3["Week 5 MOV"]
            + df_3["Week 6 MOV"]
            + df_3["Week 7 MOV"]
            + df_3["Week 8 MOV"]
            + df_3["Week 9 MOV"]
            + df_3["Week 10 MOV"]
            + df_3["Week 11 MOV"]
            + df_3["Week 12 MOV"]
            + df_3["Week 13 MOV"]
            + df_3["Week 14 MOV"]
            + df_3["Week 15 MOV"]
        ) / week_current

        df_3["Ave MOV"] = ave_column

        # total points against
        points_against = df_2["Points Scored"] - (
            df_3["Week 1 MOV"]
            + df_3["Week 2 MOV"]
            + df_3["Week 3 MOV"]
            + df_3["Week 4 MOV"]
            + df_3["Week 5 MOV"]
            + df_3["Week 6 MOV"]
            + df_3["Week 7 MOV"]
            + df_3["Week 8 MOV"]
            + df_3["Week 9 MOV"]
            + df_3["Week 10 MOV"]
            + df_3["Week 11 MOV"]
            + df_3["Week 12 MOV"]
            + df_3["Week 13 MOV"]
            + df_3["Week 14 MOV"]
            + df_3["Week 15 MOV"]
        )
        df_4 = pd.DataFrame(points_against)
        df_4.columns = ["Points Against"]

        # joins all the tables together (basically a left join)
        df = df.join(df_2)
        df = df.join(df_4)
        df = df.join(df_3)
        df = df.loc[:, (df != 0).any(axis=0)]
        df = df.sort_values(["Wins", "Points Scored"], ascending=(False, False))
        self.dataFrame = df
        return self.dataFrame

    def number_of_players(self):

        self.player_count = self.dataFrame[self.dataFrame.columns[0]].count()

        return self.player_count

    def write_table(self):
        # writes table to sql db
        if self.dataFrame == None:
            self.leagueInfo()
            self.number_of_players()
        else:
            pass

        conn = sqlite3.connect("FantasyData.sqlite")
        c = conn.cursor()
        c.execute(
            f"CREATE TABLE IF NOT EXISTS standings_{self.league_year} "
            f"(team_name text, "
            f"wins number, "
            f"losses number,"
            f"week_1 number,"
            f"week_2 number, "
            f"week_3 number, "
            f"week_4 number, "
            f"week_5 number, "
            f"week_6 number, "
            f"week_7 number, "
            f"week_8 number, "
            f"week_9 number, "
            f"week_10 number, "
            f"week_11 number, "
            f"week_12 number, "
            f"week_13 number, "
            f"week_14 number, "
            f"week_15 number, "
            f"week_16 number, "
            f"week_17 number, "
            f"points_scored number, "
            f"points_against number, "
            f"week_1_MOV number, "
            f"week_2_MOV number, "
            f"week_3_MOV number, "
            f"week_4_MOV number, "
            f"week_5_MOV number, "
            f"week_6_MOV number, "
            f"week_7_MOV number, "
            f"week_8_MOV number, "
            f"week_9_MOV number, "
            f"week_10_MOV number, "
            f"week_11_MOV number, "
            f"week_12_MOV number, "
            f"week_13_MOV number, "
            f"week_14_MOV number, "
            f"week_15_MOV number, "
            f"week_16_MOV number, "
            f"week_17_MOV number, "
            f"ave_MOV number)"
        )
        data = self.dataFrame
        self.dataFrame.to_sql(
            f"standings_{self.league_year}", conn, if_exists="replace", index=False
        )

        c.execute(
            f"""
        SELECT * FROM standings_{self.league_year}
        """
        )

    def read_table(self):
        if exists("FantasyData.sqlite") == False:
            self.write_table()
        else:
            pass

        engine = db.create_engine("sqlite:///FantasyData.sqlite")
        connection = engine.connect()
        metadata = db.MetaData()
        table_read = db.Table(
            f"standings_{self.league_year}",
            metadata,
            autoload=True,
            autoload_with=engine,
        )
        query = db.select([table_read])
        query_results = connection.execute(query)
        query_set = query_results.fetchall()

        df = pd.read_sql(f"SELECT * FROM standings_{self.league_year}", connection)
        print(df)
        print("\n")

    def least_points_scored(self, week):
        if exists("FantasyData.sqlite") == False:
            self.write_table()
        else:
            pass

        engine = db.create_engine("sqlite:///FantasyData.sqlite")
        connection = engine.connect()

        df = pd.read_sql("SELECT * " f"FROM standings_{self.league_year}", connection)

        df_low = df[f"Week {week}"].min()

        df_loser = df[(df[f"Week {week}"] == df_low)]
        df_loser = df_loser[["Team Name", f"Week {week}"]]

        print(f"Week {week} least points scored")
        print(df_loser.to_string(index=False))
        print("\n")

    def most_points_scored(self, week):
        if exists("FantasyData.sqlite") == False:
            self.write_table()
        else:
            pass

        engine = db.create_engine("sqlite:///FantasyData.sqlite")
        connection = engine.connect()

        df = pd.read_sql("SELECT * " f"FROM standings_{self.league_year}", connection)

        df_low = df[f"Week {week}"].max()

        df_loser = df[(df[f"Week {week}"] == df_low)]
        df_loser = df_loser[["Team Name", f"Week {week}"]]

        print(f"Week {week} most points scored")
        print(df_loser.to_string(index=False))
        print("\n")

    def most_narrow_win(self, week):
        if exists("FantasyData.sqlite") == False:
            self.write_table()
        else:
            pass

        engine = db.create_engine("sqlite:///FantasyData.sqlite")
        connection = engine.connect()

        df = pd.read_sql("SELECT * " f"FROM standings_{self.league_year}", connection)

        df_low = df[df[f"Week {week} MOV"] > 0]

        df_narrow = df_low[f"Week {week} MOV"].min()

        df_winner = df[(df[f"Week {week} MOV"] == df_narrow)]
        df_winner = df_winner[["Team Name", f"Week {week} MOV"]]
        df_winner_team = df_winner["Team Name"]
        df_winner_amount = df_winner[f"Week {week} MOV"]
        winner = df_winner_team.to_string(index=False)
        winner_MOV = df_winner_amount.to_string(index=False)

        df_loser = df[(df[f"Week {week} MOV"] == (df_narrow * -1))]
        df_loser = df_loser["Team Name"]
        loser = df_loser.to_string(index=False)

        print(f"Week {week} most narrow win\n" f"{winner} beat {loser} by {winner_MOV}")
        print("\n")
        # print(df_winner.to_string(index=False))

    def largest_MOV(self, week):
        if exists("FantasyData.sqlite") == False:
            self.write_table()
        else:
            pass

        engine = db.create_engine("sqlite:///FantasyData.sqlite")
        connection = engine.connect()

        df = pd.read_sql("SELECT * " f"FROM standings_{self.league_year}", connection)

        df_high = df[f"Week {week} MOV"].max()

        df_winner = df[(df[f"Week {week} MOV"] == df_high)]
        df_winner = df_winner[["Team Name", f"Week {week} MOV"]]
        df_winner_team = df_winner["Team Name"]
        df_winner_amount = df_winner[f"Week {week} MOV"]
        winner = df_winner_team.to_string(index=False)
        winner_MOV = df_winner_amount.to_string(index=False)

        df_loser = df[(df[f"Week {week} MOV"] == (df_high * -1))]
        df_loser = df_loser[["Team Name", f"Week {week} MOV"]]
        df_loser_team = df_loser["Team Name"]
        loser = df_loser_team.to_string(index=False)

        print(
            f"Week {week} Largest MOV\n"
            f"{winner} defeated {loser} by {winner_MOV} points"
        )
        print("\n")
        # print(df_loser.to_string(index=False))

    def most_efficient_team(self):
        if exists("FantasyData.sqlite") == False:
            self.write_table()
        else:
            pass

        engine = db.create_engine("sqlite:///FantasyData.sqlite")
        connection = engine.connect()

        df = pd.read_sql("SELECT * " f"FROM standings_{self.league_year}", connection)

        df = df[["Team Name", "Ave MOV"]].sort_values(by=["Ave MOV"], ascending=True)
        df = df[df["Ave MOV"] > 0].head(1)

        print("Most Efficient Team:")
        print(df.to_string(index=False))
        print("\n")

    def most_efficient_team_by_week(self, week):
        if exists("FantasyData.sqlite") == False:
            self.write_table()
        else:
            pass

        engine = db.create_engine("sqlite:///FantasyData.sqlite")
        connection = engine.connect()

        df = pd.read_sql("SELECT * " f"FROM standings_{self.league_year}", connection)

        df = df[["Team Name", f"Week {week} MOV"]].sort_values(
            by=[f"Week {week} MOV"], ascending=True
        )
        df = df[df[f"Week {week} MOV"] > 0].head(1)

        print(f"Most Efficient Team (Week {week}):")
        print(df.to_string(index=False))
        print("\n")

    def least_efficient_team(self):
        if exists("FantasyData.sqlite") == False:
            self.write_table()
        else:
            pass

        engine = db.create_engine("sqlite:///FantasyData.sqlite")
        connection = engine.connect()

        df = pd.read_sql("SELECT * " f"FROM standings_{self.league_year}", connection)

        df = (
            df[["Team Name", "Ave MOV"]]
            .sort_values(by=["Ave MOV"], ascending=False)
            .head(1)
        )

        print("Least Efficient Team:")
        print(df.to_string(index=False))
        print("\n")

    def least_efficient_team_by_week(self, week):
        if exists("FantasyData.sqlite") == False:
            self.write_table()
        else:
            pass

        engine = db.create_engine("sqlite:///FantasyData.sqlite")
        connection = engine.connect()

        df = pd.read_sql("SELECT * " f"FROM standings_{self.league_year}", connection)

        df = (
            df[["Team Name", f"Week {week} MOV"]]
            .sort_values(by=[f"Week {week} MOV"], ascending=False)
            .head(1)
        )

        print(f"Least Efficient Team (Week {week}):")
        print(df.to_string(index=False))
        print("\n")

    def most_consistent(self):
        league = League(self.league_id, self.league_year)
        week_current = league.current_week - 1

        if exists("FantasyData.sqlite") == False:
            self.write_table()
        else:
            pass

        engine = db.create_engine("sqlite:///FantasyData.sqlite")
        connection = engine.connect()

        df = pd.read_sql("SELECT * " f"FROM standings_{self.league_year}", connection)

        df = df[
            [
                "Team Name",
                "Week 1",
                "Week 2",
                "Week 3",
                "Week 4",
                "Week 5",
                "Week 6",
                "Week 7",
            ]
        ]

        df1 = df.transpose(copy=True)
        new_header = df1.iloc[0]
        df1 = df1[1:]
        df1.columns = new_header
        df2 = df1.std()
        df2 = df2.sort_values().head(1)
        df3 = df1.mean()

        print("most consistent Team:")
        print(df2.to_string())
        print("\n")
        print(df3)


def main():
    """examples"""
    week = 8
    fantasyfootballdata = FantasyFootball(league_year=2022)
    fantasyfootballdata.write_table()
    fantasyfootballdata.most_narrow_win(week)
    fantasyfootballdata.largest_MOV(week)
    fantasyfootballdata.most_points_scored(week)
    fantasyfootballdata.least_points_scored(week)
    fantasyfootballdata.most_efficient_team()
    fantasyfootballdata.most_efficient_team_by_week(week)
    fantasyfootballdata.least_efficient_team()
    fantasyfootballdata.least_efficient_team_by_week(week)
    fantasyfootballdata.most_consistent()
    fantasyfootballdata.read_table()


if __name__ == "__main__":
    main()
