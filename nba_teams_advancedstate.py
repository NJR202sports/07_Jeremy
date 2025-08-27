
# 球隊進階數據 Advanced Stats
import urllib.request as req
import os
import bs4 as bs
import pandas as pd

def nba_teams_advancedstate(year:int):

    url = f"https://www.basketball-reference.com/leagues/NBA_{year}.html"
    resp = req.urlopen(url)
    content = resp.read()
    html = bs.BeautifulSoup(content,'html.parser')

    table = html.find("div", {"id": "all_advanced_team"}).find("tbody")
    rows = table.find_all("tr")

    teams = []
    team_seen = set()

    for row in rows:
        # 跳過空列或是有 'class=thead' 的分隔列
        if row.get("class") == ['thead']:
            continue

        # 抓球員名字與隊伍代碼
        team_cell = row.find("td", {"data-stat": "team"})
        age_cell = row.find("td", {"data-stat": "age"})
        wins_cell = row.find("td", {"data-stat": "wins"})
        losses_cell = row.find("td", {"data-stat": "losses"})
        wins_pyth_cell = row.find("td", {"data-stat": "wins_pyth"})
        losses_pyth_cell = row.find("td", {"data-stat": "losses_pyth"})
        mov_cell = row.find("td", {"data-stat": "mov"})
        sos_cell = row.find("td", {"data-stat": "sos"})
        srs_cell = row.find("td", {"data-stat": "srs"})
        off_rtg_cell = row.find("td", {"data-stat": "off_rtg"})
        def_rtg_cell = row.find("td", {"data-stat": "def_rtg"})
        net_rtg_cell = row.find("td", {"data-stat": "net_rtg"})
        pace_cell = row.find("td", {"data-stat": "pace"})
        fta_per_fga_pct_cell = row.find("td", {"data-stat": "fta_per_fga_pct"})
        fg3a_per_fga_pct_cell = row.find("td", {"data-stat": "fg3a_per_fga_pct"})
        ts_pct_cell = row.find("td", {"data-stat": "ts_pct"})
        efg_pct_cell = row.find("td", {"data-stat": "efg_pct"})
        tov_pct_cell = row.find("td", {"data-stat": "tov_pct"})
        orb_pct_cell = row.find("td", {"data-stat": "orb_pct"})
        ft_rate_cell = row.find("td", {"data-stat": "ft_rate"})
        opp_efg_pct_cell = row.find("td", {"data-stat": "opp_efg_pct"})
        opp_tov_pct_cell = row.find("td", {"data-stat": "opp_tov_pct"})
        drb_pct_cell = row.find("td", {"data-stat": "drb_pct"})
        opp_ft_rate_cell = row.find("td", {"data-stat": "opp_ft_rate"})
        arena_name_cell = row.find("td", {"data-stat": "arena_name"})
        attendance_cell = row.find("td", {"data-stat": "attendance"})
        attendance_per_g_cell = row.find("td", {"data-stat": "attendance_per_g"})


        team = team_cell.text.strip()
        age = age_cell.text.strip()
        wins = wins_cell.text.strip()
        losses = losses_cell.text.strip()
        wins_pyth = wins_pyth_cell.text.strip()
        losses_pyth = losses_pyth_cell.text.strip()
        mov = mov_cell.text.strip()
        sos = sos_cell.text.strip()
        srs = srs_cell.text.strip()
        off_rtg = off_rtg_cell.text.strip()
        def_rtg = def_rtg_cell.text.strip()
        net_rtg = net_rtg_cell.text.strip()
        pace = pace_cell.text.strip()
        fta_per_fga_pct = fta_per_fga_pct_cell.text.strip()
        fg3a_per_fga_pct = fg3a_per_fga_pct_cell.text.strip()
        ts_pct = ts_pct_cell.text.strip()
        efg_pct = efg_pct_cell.text.strip()
        tov_pct = tov_pct_cell.text.strip()
        orb_pct = orb_pct_cell.text.strip()
        ft_rate = ft_rate_cell.text.strip()

        opp_efg_pct = opp_efg_pct_cell.text.strip()
        opp_tov_pct = opp_tov_pct_cell.text.strip()
        drb_pct = drb_pct_cell.text.strip()
        opp_ft_rate = opp_ft_rate_cell.text.strip()

        arena_name = arena_name_cell.text.strip()
        attendance = attendance_cell.text.strip()
        attendance_per_g = attendance_per_g_cell.text.strip()



        if team not in team_seen:
            teams.append({
                "team": team,
                "average_age": age,
                "wins": wins,
                "loses": losses,
                "pythagorean_wins": wins_pyth,
                "pythagorean_lose": losses_pyth,
                "margin_of_victory": mov,
                "strength_of_schedule": sos,
                "simple_rating_system": srs,
                "offensive_rating": off_rtg,
                "defensive_rating": def_rtg,
                "net_rating": net_rtg,
                "pace_factor": pace,
                "free_throw_attempt_rate": fta_per_fga_pct,
                "3p_attempt_rate": fg3a_per_fga_pct,
                "true_shooting_percentage": ts_pct,
                "effective_field_goal_percentage": efg_pct,
                "turnover_percentage": tov_pct,
                "offensive_rebound_percentage": orb_pct,
                "free_throws_per_field_goal_attempt": ft_rate,

                "opponent_effective_field_goal_percentage": opp_efg_pct,
                "opponent_turnover_percentage": opp_tov_pct,
                "defensive_rebound_percentage": drb_pct,
                "opponent_free_throws_per_field_goal_attempt": opp_ft_rate,

                "球場": arena_name,
                "總入場人數": attendance,
                "平均每場比賽入場人數": attendance_per_g,

                })
            team_seen.add(team)

    dirname = "nba_teams_advancedstate"
    if not os.path.exists(dirname):
        os.mkdir(dirname)


    df = pd.DataFrame(teams)
    df.index += 1
    fn = os.path.join(dirname, f"nba_teams_advancedstate_{year}.csv")
    df.to_csv(fn, encoding="utf-8-sig")


print(nba_teams_advancedstate(2023))    