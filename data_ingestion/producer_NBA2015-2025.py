from data_ingestion.tasks_nba_players_salary import nba_players_salary
from data_ingestion.tasks_nba_players_state import nba_players_state
from data_ingestion.tasks_nba_teams_advancedstate import nba_teams_advancedstate
from data_ingestion.tasks_nba_teams_salary import nba_teams_salary
from data_ingestion.tasks_nba_teams_state import nba_teams_state

years = list(range(2015,2016))

for year in years:

    nba_players_salary.delay(year=year)
    print(f"send task to nba_players_salary: {year}")
    nba_players_state.delay(year=year)
    print(f"send task to nba_players_state: {year}")
    nba_teams_advancedstate.delay(year=year)
    print(f"send task to nba_teams_advancedstate: {year}")
    nba_teams_salary.delay(year=year)
    print(f"send task to nba_teams_salary: {year}")
    nba_teams_state.delay(year=year)
    print(f"send task to nba_teams_state: {year}")