import os
import re
import json
import requests
import datetime
from datetime import timedelta

import pandas as pd

from s3utils import publish_to_s3

S3_BUCKET_INTERNAL_NOTEBOOK_DATA = os.environ.get("S3_BUCKET_INTERNAL_NOTEBOOK_DATA")
S3_BUCKET_PUBLIC = os.environ.get("S3_BUCKET_PUBLIC")

DATO_RE = re.compile("(\d{6})")
CALCULATOR_TEAMS = ["Brann"]
SEASON_ID = "6335"


def find_brann_id(schedule_json):
    for key, val in schedule_json["participants"].items():
        if val["name"] == "Brann":
            return key


def normalize_name(name):
    """
    Makes sure that names are written the same way
    name (str): name to normalize

    return (str): normalized name
    """
    name = name.replace("IK Start", "Start")
    name = name.replace("Kongsvinger IL", "Kongsvinger")
    name = name.replace("Molde FK", "Molde")
    name = name.replace("Bodø / Glimt", "Bodø/Glimt")
    name = name.replace("Strømsgodset IF", "Strømsgodset")
    name = name.replace("Stabæk Fotball", "Stabæk")
    name = name.replace("Tromsø IL", "Tromsø")
    name = name.replace("Lillestrøm SK", "Lillestrøm")
    name = name.replace("Hødd IL", "Hødd")
    name = name.replace("Viking FK", "Viking")
    name = name.replace("Moss FK", "Moss")
    name = name.replace("Kristiansund BK", "Kristiansund")
    name = name.replace("Ranheim TF", "Ranheim")
    name = name.replace("Odd MAX", "Odd")
    name = name.replace("Sandefjord Fotball", "Sandefjord")
    name = name.replace("FK Haugesund", "Haugesund")
    name = name.replace("Mjøndalen IF", "Mjøndalen")
    name = name.replace("FK Eik-Tønsberg", "Eik-Tønsberg")
    name = name.replace("Sarpsborg FK", "Sarpsborg")
    name = name.replace("Tromsdalen UIL", "Tromsdalen")
    name = name.replace("FK Mandalskameratene", "Mandalskameratene")
    name = name.replace("FK Ørn-Horten", "Ørn-Horten")
    name = name.replace("Tollnes BK", "Tollnes")
    name = name.replace("FK Manglerud Star", "Manglerud Star")
    name = name.replace("Stavanger IF", "Stavanger")
    name = name.replace("Levanger FK", "Levanger")
    name = name.replace("KFUM Oslo", "KFUM")
    name = name.replace("Florø SK", "Florø")
    name = name.replace("Øygarden FK", "Øygarden")

    return name


"""
Bunch of functions to calculate stuff
"""


def calculate_ponts(x, team):
    if x.result == "x":
        return 1

    if x.home_team == team:
        if x.result == "1":
            return 3
        if x.result == "2":
            return 0

    if x.away_team == team:
        if x.result == "1":
            return 0
        if x.result == "2":
            return 3

    return -1


def find_home_away(x, team):
    if x.home_team == team:
        return "home"
    elif x.away_team == team:
        return "away"

    return "unknown"


def find_opponent_team(x, team):
    if x.home_team == team:
        return x.away_team
    elif x.away_team == team:
        return x.home_team


def team_goals(x, team):
    if x.home_team == team:
        return x.home_goals_full_time
    elif x.away_team == team:
        return x.away_goals_full_time


def team_conceded(x, team):
    if x.home_team == team:
        return x.away_goals_full_time
    elif x.away_team == team:
        return x.home_goals_full_time


def team_goals_half_time(x, team):
    if x.home_team == team:
        return x.home_goals_half_time
    elif x.away_team == team:
        return x.away_goals_half_time


def team_conceded_half_time(x, team):
    if x.home_team == team:
        return x.away_goals_half_time
    elif x.away_team == team:
        return x.home_goals_half_time


def half_time_full_time(x):
    result = ""
    if x.team_goals_half_time < x.team_conceded_half_time:
        result = "l"
    if x.team_goals_half_time == x.team_conceded_half_time:
        result = "d"
    if x.team_goals_half_time > x.team_conceded_half_time:
        result = "w"

    if x.team_goals < x.team_conceded:
        result += "l"
    if x.team_goals == x.team_conceded:
        result += "d"
    if x.team_goals > x.team_conceded:
        result += "w"

    return result


def non_win(x):
    if x.points == 3:
        return False

    return True


def non_loose(x):
    if x.points == 0:
        return False

    return True


def turn_around_win(x):
    if x.team_goals_half_time < x.team_conceded_half_time:
        if x.team_goals > x.team_conceded:
            return True
        else:
            return False


def turn_around_loose(x):
    if x.team_goals_half_time > x.team_conceded_half_time:
        if x.team_goals < x.team_conceded:
            return True
        else:
            return False


def generate_finished_data(data):
    matches = []

    participants = data["participants"]
    for event in data["events"]:
        if (event["tournament"]["phase"] == "group") & (
            event["status"]["type"] == "finished"
        ):
            dt = datetime.datetime.strptime(event["startDate"], "%Y-%m-%dT%H:%M:%SZ")
            home_team = event["participantIds"][0]
            away_team = event["participantIds"][1]

            home_team_name = normalize_name(participants[home_team]["name"])
            away_team_name = normalize_name(participants[away_team]["name"])

            result = "x"
            if "winnerId" in event["winners"]:
                result = (
                    "1" if str(event["winners"]["winnerId"]) == str(home_team) else "2"
                )

            match = {
                "date": event["startDate"],
                "year": dt.year,
                "month": dt.month,
                "day": dt.isoweekday(),
                "_round": int(
                    event["tournament"]["round"]
                ),  # the word round is reseverd in pandas, so add _ at the start
                "home_team": home_team_name,
                "away_team": away_team_name,
                "home_goals_half_time": event["results"][home_team]["runningScore"]
                - event["results"][home_team]["period2Score"]
                if "period2Score" in event["results"][home_team]
                else 1,  # one match lacks second half socre. And that's Rbk - Brann from 2021
                "away_goals_half_time": event["results"][away_team]["runningScore"]
                - event["results"][away_team]["period2Score"]
                if "period2Score" in event["results"][away_team]
                else 0,
                "home_goals_full_time": event["results"][home_team]["runningScore"],
                "away_goals_full_time": event["results"][away_team]["runningScore"],
                "result": result,
            }

            match[home_team_name] = True
            match[away_team_name] = True
            matches.append(match)

    _df = pd.DataFrame.from_dict(matches)
    return _df


def generate_remaining_data(data):
    matches = []

    participants = data["participants"]
    for event in data["events"]:
        if (event["tournament"]["phase"] == "group") & (
            event["status"]["type"] == "notStarted"
        ):
            dt = datetime.datetime.strptime(event["startDate"], "%Y-%m-%dT%H:%M:%SZ")
            home_team = event["participantIds"][0]
            away_team = event["participantIds"][1]

            home_team_name = normalize_name(participants[home_team]["name"])
            away_team_name = normalize_name(participants[away_team]["name"])

            match = {
                "date": event["startDate"],
                "year": dt.year,
                "month": dt.month,
                "day": dt.isoweekday(),
                "home_team": home_team_name,
                "away_team": away_team_name,
                "_round": int(
                    event["tournament"]["round"]
                ),  # the word round is reseverd in pandas, so add _ at the start
            }

            match[home_team_name] = True
            match[away_team_name] = True
            matches.append(match)

    _df = pd.DataFrame.from_dict(matches)
    _df["date"] = _df["date"].astype("datetime64[ns]")
    return _df


def get_season_standings(season_id):
    standing_response = requests.request(
        url=f"https://api.sportsnext.schibsted.io/v1/nor/tournaments/seasons/{season_id}/standings",
        method="GET",
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "bt-bord4",
        },
    )
    standing_json = json.loads(standing_response.text)
    participants = standing_json["participants"]
    standings = []
    for standing in standing_json["standings"][0]["teamStandings"]:
        standings.append(
            {
                "team": normalize_name(participants[standing["teamId"]]["name"]),
                "season_final_rank": int(standing["rank"]),
                "season_final_rule": standing["rule"]["code"]
                if "rule" in standing
                else "",
                "season_final_games": standing["played"],
                "season_final_points": standing["points"],
                "season_final_wins": standing["wins"],
                "season_final_draws": standing["draws"],
                "season_final_losses": standing["losses"],
                "season_final_goals_for": standing["goalsFor"],
                "season_final_goals_against": standing["goalsAgainst"],
            }
        )
    _df = pd.DataFrame.from_dict(standings)

    return _df


def get_threshold_for_round(_df):
    thresholds = {}

    thresholds["f"] = int(_df.iloc[0]["season_round_points_total"])
    thresholds["p"] = int(_df.iloc[2]["season_round_points_total"])
    thresholds["q_p"] = int(_df.iloc[6]["season_round_points_total"])
    thresholds["q_r"] = int(_df.iloc[12]["season_round_points_total"])
    thresholds["r"] = int(_df.iloc[13]["season_round_points_total"])
    thresholds["l"] = int(_df.iloc[15]["season_round_points_total"])

    return thresholds


def get_threshold_data(_df_season, _df_historical):
    thresholds = {}
    thresholds["seasons"] = {}

    for y in range(2012, 2022):
        thresholds["seasons"][y] = {}
        for r in range(1, 31):
            thresholds["seasons"][y][r] = {}
            _df = (
                _df_historical[(_df_historical._round == r) & (_df_historical.year == y)]
                .sort_values(
                    [
                        "season_round_points_total",
                        "season_round_goal_diff_total",
                        "season_round_scored_total",
                    ],
                    ascending=False,
                )[
                    [
                        "_round",
                        "team",
                        "season_round_scored_total",
                        "season_round_conceded_total",
                        "season_round_goal_diff_total",
                        "season_round_points_total",
                    ]
                ]
                .reset_index()
            )
            if len(_df) == 16:
                thresholds["seasons"][y][r] = get_threshold_for_round(_df)
            else:
                print(y, r, len(_df))

    thresholds["compare"] = {}
    thresholds["compare"]["aafk_2019_points"] = (
        _df_historical[(_df_historical.year == 2019) & (_df_historical.team == "Aalesund")]
        .sort_values("_round")["season_points_total"]
        .values.tolist()
    )
    thresholds["compare"]["brann_2015_points"] = (
        _df_historical[(_df_historical.year == 2015) & (_df_historical.team == "Brann")]
        .sort_values("_round")["season_points_total"]
        .values.tolist()
    )
    thresholds["compare"]["aafk_2019_results"] = (
        _df_historical[(_df_historical.year == 2019) & (_df_historical.team == "Aalesund")]
        .sort_values("_round")["points"]
        .values.tolist()
    )
    thresholds["compare"]["brann_2015_results"] = (
        _df_historical[(_df_historical.year == 2015) & (_df_historical.team == "Brann")]
        .sort_values("_round")["points"]
        .values.tolist()
    )

    thresholds["current"] = {}
    for r in _df_season.sort_values("_round")._round.unique():
        _df = (
            _df_season[_df_season._round == r]
            .sort_values(
                [
                    "season_round_points_total",
                    "season_round_goal_diff_total",
                    "season_round_scored_total",
                ],
                ascending=False,
            )[
                [
                    "_round",
                    "team",
                    "season_round_scored_total",
                    "season_round_conceded_total",
                    "season_round_goal_diff_total",
                    "season_round_points_total",
                ]
            ]
            .reset_index()
        )
        if len(_df) == 16:
            thresholds["current"][int(r)] = get_threshold_for_round(_df)

    return json.dumps(thresholds)


def get_historical():
    df_historical = pd.read_csv(
        "https://www.bt.no/data/sport/brann/historical_data_obos/historical_data_obos_latest.csv"
    )

    df_historical = df_historical[df_historical.year > 2011]
    df_historical["date"] = df_historical["date"].astype("datetime64[ns]")
    df_historical = df_historical.round(2)
    df_historical["season_goal_diff_total"] = (
        df_historical["season_scored_total"] - df_historical["season_conceded_total"]
    )
    df_historical["season_round_goal_diff_total"] = (
        df_historical["season_round_scored_total"]
        - df_historical["season_round_conceded_total"]
    )

    return df_historical


def generate_season_data(df_finished):
    team_dfs = []
    for team in df_finished.home_team.unique():
        _df = df_finished[df_finished[team] == True].copy().reset_index()
        _df["team"] = team
        _df = _df.sort_values("date", ascending=True)
        _df["opponent_team"] = _df.apply(lambda x: find_opponent_team(x, team), axis=1)
        _df["home_away"] = _df.apply(lambda x: find_home_away(x, team), axis=1)
        _df["season_game"] = _df.groupby(["year"]).cumcount() + 1
        _df["points"] = _df.apply(lambda x: calculate_ponts(x, team), axis=1)
        _df["team_goals"] = _df.apply(lambda x: team_goals(x, team), axis=1)
        _df["team_conceded"] = _df.apply(lambda x: team_conceded(x, team), axis=1)
        _df["team_goals_half_time"] = _df.apply(
            lambda x: team_goals_half_time(x, team), axis=1
        )
        _df["team_conceded_half_time"] = _df.apply(
            lambda x: team_conceded_half_time(x, team), axis=1
        )
        _df["non_win"] = _df.apply(lambda x: non_win(x), axis=1)
        _df["non_loose"] = _df.apply(lambda x: non_loose(x), axis=1)
        _df["turn_around_win"] = _df.apply(lambda x: turn_around_win(x), axis=1)
        _df["turn_around_loose"] = _df.apply(lambda x: turn_around_loose(x), axis=1)
        _df["half_time_full_time"] = _df.apply(lambda x: half_time_full_time(x), axis=1)

        _df["season_points_total"] = _df.groupby(["year"])["points"].apply(
            lambda x: x.cumsum()
        )
        _df["season_points_avg"] = _df["season_points_total"] / _df["season_game"]
        _df["season_points_avg_before"] = _df["season_points_avg"].shift(1)
        _df["season_scored_total"] = _df.groupby(["year"])["team_goals"].apply(
            lambda x: x.cumsum()
        )
        _df["season_conceded_total"] = _df.groupby(["year"])["team_conceded"].apply(
            lambda x: x.cumsum()
        )

        _df.loc[_df.home_away == "home", "season_home_game"] = (
            _df[_df.home_away == "home"].groupby(["year"]).cumcount() + 1
        )
        _df.loc[_df.home_away == "home", "season_home_points_total"] = (
            _df[_df.home_away == "home"]
            .groupby(["year"])["points"]
            .apply(lambda x: x.cumsum())
        )
        _df.loc[_df.home_away == "home", "season_home_points_avg"] = (
            _df[_df.home_away == "home"]["season_home_points_total"]
            / _df[_df.home_away == "home"]["season_home_game"]
        )
        _df.loc[_df.home_away == "home", "season_home_points_avg_before"] = _df[
            _df.home_away == "home"
        ]["season_home_points_avg"].shift(1)

        _df.loc[_df.home_away == "away", "season_away_game"] = (
            _df[_df.home_away == "away"].groupby(["year"]).cumcount() + 1
        )
        _df.loc[_df.home_away == "away", "season_away_points_total"] = (
            _df[_df.home_away == "away"]
            .groupby(["year"])["points"]
            .apply(lambda x: x.cumsum())
        )
        _df.loc[_df.home_away == "away", "season_away_points_avg"] = (
            _df[_df.home_away == "away"]["season_away_points_total"]
            / _df[_df.home_away == "away"]["season_away_game"]
        )
        _df.loc[_df.home_away == "away", "season_away_points_avg_before"] = _df[
            _df.home_away == "away"
        ]["season_away_points_avg"].shift(1)

        _df["form_6_avg"] = _df["points"].rolling(6).mean()
        _df["form_30_avg"] = _df["points"].rolling(30).mean()

        _df_home = _df[_df.home_away == "home"]
        _df_away = _df[_df.home_away == "away"]

        _df = _df.sort_values("date", ascending=False)
        _df["season_game_reverse"] = _df.groupby(["year"]).cumcount() + 1
        _df["season_points_total_reverse"] = _df.groupby(["year"])["points"].apply(
            lambda x: x.cumsum()
        )
        _df["season_points_avg_reverse"] = (
            _df["season_points_total_reverse"] / _df["season_game_reverse"]
        )
        _df["season_points_avg_after"] = _df["season_points_avg_reverse"].shift(1)
        _df["season_scored_total_reverse"] = _df.groupby(["year"])["team_goals"].apply(
            lambda x: x.cumsum()
        )
        _df["season_conceded_total_reverse"] = _df.groupby(["year"])[
            "team_conceded"
        ].apply(lambda x: x.cumsum())

        _df.loc[_df.home_away == "home", "season_home_game_reverse"] = (
            _df[_df.home_away == "home"].groupby(["year"]).cumcount() + 1
        )
        _df.loc[_df.home_away == "home", "season_home_points_total_reverse"] = (
            _df[_df.home_away == "home"]
            .groupby(["year"])["points"]
            .apply(lambda x: x.cumsum())
        )
        _df.loc[_df.home_away == "home", "season_home_points_avg_reverse"] = (
            _df[_df.home_away == "home"]["season_home_points_total_reverse"]
            / _df[_df.home_away == "home"]["season_home_game_reverse"]
        )
        _df.loc[_df.home_away == "home", "season_home_points_avg_after"] = _df[
            _df.home_away == "home"
        ]["season_home_points_avg_reverse"].shift(1)

        _df.loc[_df.home_away == "away", "season_away_game_reverse"] = (
            _df[_df.home_away == "away"].groupby(["year"]).cumcount() + 1
        )
        _df.loc[_df.home_away == "away", "season_away_points_total_reverse"] = (
            _df[_df.home_away == "away"]
            .groupby(["year"])["points"]
            .apply(lambda x: x.cumsum())
        )
        _df.loc[_df.home_away == "away", "season_away_points_avg_reverse"] = (
            _df[_df.home_away == "away"]["season_away_points_total_reverse"]
            / _df[_df.home_away == "away"]["season_away_game_reverse"]
        )
        _df.loc[_df.home_away == "away", "season_away_points_avg_after"] = _df[
            _df.home_away == "away"
        ]["season_away_points_avg_reverse"].shift(1)

        _df = _df.sort_values("_round", ascending=True)
        _df["season_round_points_total"] = _df.groupby(["year"])["points"].apply(
            lambda x: x.cumsum()
        )
        _df["season_round_points_avg"] = (
            _df["season_round_points_total"] / _df["_round"]
        )
        _df["season_round_scored_total"] = _df.groupby(["year"])["team_goals"].apply(
            lambda x: x.cumsum()
        )
        _df["season_round_conceded_total"] = _df.groupby(["year"])[
            "team_conceded"
        ].apply(lambda x: x.cumsum())

        team_dfs.append(_df)

    df_season = pd.concat(team_dfs)
    df_season = df_season.reset_index(drop=True)

    df_season["season_round_goal_diff_total"] = (
        df_season["season_round_scored_total"]
        - df_season["season_round_conceded_total"]
    )

    return df_season


def get_brann_position(standings):
    return standings[standings.team == "Brann"]["rank"].values[0]


def get_teams_positions(_df, teams=["Brann"]):
    _df = _df[_df.team.isin(teams)][
        ["team", "season_final_rank", "season_final_points"]
    ]
    _df.columns = ["team", "rank", "points"]
    return _df


def get_similar(_df_historical, avg, number_of_matches_played):
    _df = _df_historical[
        (_df_historical._round == number_of_matches_played)
    ].sort_values("season_points_avg", ascending=False)[
        [
            "team",
            "year",
            "season_round_points_avg",
            "season_round_points_total",
            "season_final_rank",
            "season_final_avg",
            "season_final_rule",
            "season_final_points",
        ]
    ]

    return _df[(_df.season_round_points_avg >= avg)].to_json(orient="records")


def get_upcoming_matches(_df, teams=["Brann"]):
    return _df[(_df.home_team.isin(teams)) | (_df.away_team.isin(teams))][
        ["date", "home_team", "away_team", "_round"]
    ].to_json(orient="records")


def generate_json(_df_finished, _df_remaining):
    total_number_of_matches_played = _df_finished.shape[0]

    _df_historical = get_historical()
    _df_season = generate_season_data(_df_finished)
    _df_brann = _df_season[_df_season.team == "Brann"][
        [
            "date",
            "_round",
            "season_game",
            "opponent_team",
            "home_away",
            "points",
            "season_round_points_total",
            "season_round_scored_total",
            "season_round_conceded_total",
            "form_6_avg",
            "season_round_points_avg",
            "team_goals",
            "team_conceded",
        ]
    ].sort_values("date", ascending=True)

    brann_number_of_matches_played = len(_df_brann.season_game)

    brann_avg = float(_df_brann.iloc[-1]["season_round_points_avg"])
    brann_points = int(_df_brann.iloc[-1]["season_round_points_total"])
    similar = get_similar(_df_historical, brann_avg, brann_number_of_matches_played)

    season_standings = get_season_standings(SEASON_ID)
    coming_matches_teams = CALCULATOR_TEAMS
    teams_positions = get_teams_positions(season_standings, coming_matches_teams)

    return f""" {{
        "generated": "{datetime.datetime.now().isoformat()}",
        "report_name": "Brann dashboard",
        "author": "BT Bord 4/Lasse Lambrechts",
        "source": "100% Sport",
        "data": {{
            "total_number_of_matches_played": "{total_number_of_matches_played}",
            "thresholds": {get_threshold_data(_df_season, _df_historical)},
            "brann": {{
                "number_of_matches": "{brann_number_of_matches_played}",
                "position": {get_brann_position(teams_positions)},
                "avg": {brann_avg},
                "points": {brann_points},
                "seasong_avgs": {_df_brann.season_round_points_avg.to_list()},
                "season_games": {_df_brann.to_json(orient="records")}
            }},
            "standings": {season_standings.to_json(orient="records")},
            "similar": {{
                "list": {similar}
            }},
            "coming_matches": {{
                "matches": {get_upcoming_matches(_df_remaining, coming_matches_teams)},
                "teams": {teams_positions.to_json(orient="records")}
            }}
        }}}}
        """.replace(
        "\n", ""
    )



schedule_response = requests.request(
    url=f"https://api.sportsnext.schibsted.io/v1/nor/tournaments/seasons/{SEASON_ID}/schedule",
    method="GET",
    headers={
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "bt-bord4",
    },
)
schedule_json = json.loads(schedule_response.text)
df_finished = generate_finished_data(schedule_json)
df_remaining = generate_remaining_data(schedule_json)

df_finished["date"] = df_finished["date"].astype("datetime64[ns]")
df_remaining["date"] = df_remaining["date"].astype("datetime64[ns]")

brann_dashboard_json = generate_json(df_finished, df_remaining)

publish_to_s3(
    "bt-board4",
    "data/sports/brann",
    "brann_dashboard",
    datetime.datetime.today().strftime("%Y-%m-%d"),
    brann_dashboard_json,
)
