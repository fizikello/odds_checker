from bs4 import BeautifulSoup
import requests
import warnings
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine, MetaData, Table, select, Integer, String
from hidden_values import Secrets
import random
import string
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import time
from datetime import datetime, timedelta
import locale

#path = 'https://www.efortuna.pl/zaklady-bukmacherskie/pilka-nozna/ekstraklasa-polska'
path = 'https://www.efortuna.pl/zaklady-bukmacherskie/pika-nozna/polska-3/ekstraklasa-polska?tab=matches&filter=all'
#path = 'https://www.efortuna.pl/zaklady-bukmacherskie/pilka-nozna/polska-1-liga'
s = Secrets()

TIME_SLEEP = 8
PARAMETER = 'Ekstraklasa Polska'

def event_id_to_bigint(event_id_str):
    """Zamienia np. 'ufo:mtch:1kp-01s' na liczbę całkowitą na podstawie ASCII znaków."""
    clean = event_id_str.replace("ufo:mtch:", "").replace("-", "")
    bigint = ""
    for c in clean:
        if c.isdigit():
            bigint += c
        else:
            bigint += f"{ord(c):03d}"  # 3 cyfry na każdą literę, np. 'k' = 107
    return int(bigint)


def check_if_new_teams_are_in_data(teams, parameter='home'):
    meta_parameter = 'away' if parameter == 'home' else 'home'

    test_home_join = pd.merge(left=df_from_scrap, right=teams, how='outer', left_on=[parameter],
                              right_on=['NAME_FORTUNA'], indicator=True)
    test_home_join = test_home_join[test_home_join['_merge'] == 'left_only']

    max_index_value = teams["ID"].max()
    new_teams = pd.DataFrame(test_home_join)
    new_teams.drop(
        ['event-id', '1', 'X', '2', 'event-datetime', meta_parameter, 'update-date', 'NAME_FORTUNA', '_merge'],
        axis='columns', inplace=True)

    new_teams["ID"] = max_index_value + 1 + new_teams.index
    new_teams.rename(columns={parameter: "NAME_FORTUNA"}, inplace=True)
    new_teams = pd.concat([teams, new_teams], ignore_index=False)

    return new_teams


def check_if_new_dates_in_data(calendar):
    # Upewnij się, że kluczowe kolumny mają ten sam typ danych
    df_from_scrap["event-datetime"] = df_from_scrap["event-datetime"].astype(str)
    calendar["DATE"] = calendar["DATE"].astype(str)

    # Łączenie danych
    test_join = pd.merge(
        left=df_from_scrap,
        right=calendar,
        how='outer',
        left_on=['event-datetime'],
        right_on=['DATE'],
        indicator=True
    )
    test_join = test_join[test_join['_merge'] == 'left_only']

    # Przygotowanie nowych rekordów
    max_index_value = calendar["ID"].max()
    new_calendar = pd.DataFrame(test_join)

    new_calendar.drop(
        ['event-id', '1', 'X', '2', 'away', 'home', 'update-date', '_merge'],
        axis='columns',
        inplace=True,
        errors='ignore'  # Dodano na wypadek, gdyby kolumny nie istniały
    )

    new_calendar["ID"] = max_index_value + new_calendar.index + 1
    new_calendar['DATE'] = new_calendar['DATE'].fillna(new_calendar['event-datetime'])
    del new_calendar['event-datetime']

    # Łączenie z istniejącym kalendarzem
    new_calendar = pd.concat([calendar, new_calendar], ignore_index=True)

    return new_calendar

def check_calendar_id(date_from_fortuna):
    # engine = create_engine(f'postgresql+psycopg2://{s.user}:{s.password}@{s.host}:{s.port}/{s.dbname}')
    connection = psycopg2.connect(dbname=s.dbname, user=s.user, password=s.password, host=s.host, port=s.port)

    unikalny_id = ''.join(random.choices(string.digits, k=6))
    row_data = [unikalny_id, date_from_fortuna]
    cursor = connection.cursor()
    sql_command = f'INSERT INTO calendar ("ID", "DATE") VALUES (%s, %s) ON CONFLICT ("DATE") DO NOTHING;'

    cursor.execute(sql_command, row_data)
    connection.commit()
    connection.close()
    # with engine.connect() as connection:
    #    connection.execute(zapytanie, (unikalny_id, date_from_fortuna))


def check_team_id(team_name):
    engine = create_engine(f'postgresql+psycopg2://{s.user}:{s.password}@{s.host}:{s.port}/{s.dbname}')
    metadata = MetaData()
    table_teams = Table('teams', metadata, autoload_with=engine)
    sql_get_name = select(table_teams.c.ID).where(table_teams.c.NAME_FORTUNA == team_name)
    tbr = None

    with engine.connect() as connection:
        wyniki = connection.execute(sql_get_name)
        for wiersz in wyniki:
            print(f"Klucz dla wartości {team_name}:", wiersz[0])
            tbr = wiersz[0]

        wyniki.close()
    return tbr


def extract_data():
    driver = webdriver.Chrome()
    driver.maximize_window()

    driver.get("https://www.efortuna.pl/")
    time.sleep(3)

    driver.execute_script("""
    var overlay = document.getElementById('cookie-consent-overlay');
    if(overlay) {
      overlay.style.display = 'none';
    }
    """)

    time.sleep(1)

    try:
        ekstraklasa = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((
            By.XPATH, "//div[contains(@class, 'side-menu-item__title') and contains(text(), 'Ekstraklasa Polska')]"
            # By.XPATH, "//div[contains(@class, 'side-menu-item__title') and contains(text(), 'Polska 1.')]"
        )))
        driver.execute_script("arguments[0].scrollIntoView();", ekstraklasa)
        ekstraklasa.click()
        print("Kliknięto w Ekstraklasa Polska!")
        time.sleep(TIME_SLEEP)

        kursy_div = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span.odds-button__value-current"))
        )

        """
        kursy_div = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "#main-container__content > div > main > div:nth-child(1) > div.grow.offer-tournament-overview-container.flex.flex-col.gap-8")
            )
        )
        print("Załadowano div z kursami!")
         html = driver.page_source
        with open("efortuna_ekstraklasa.html", "w", encoding="utf-8") as f:
            f.write(html)
        """
        html = driver.page_source
        with open("efortuna_ekstraklasa.html", "w", encoding="utf-8") as f:
            f.write(html)
        # Pobierz HTML tego diva
        # html = kursy_div.get_attribute('outerHTML')

        # with open("test_page.html", "w", encoding="utf-8") as f:
        #    f.write(html)

        # 4. Parsuj przez BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()

        # Szukamy wszystkich kart meczowych
        fixtures = soup.find_all("a", class_="fixture-safe-link")
        print(f"fixtures: {len(fixtures)}")
        matches = []
        for fixture in fixtures:

            event_id_raw = fixture.get("data-id")  # np. ufo:mtch:1kp-029
            event_id = None
            if event_id_raw:
                # Ekstra: wyciągnięcie samego identyfikatora (np. 1kp-029)
                match = re.search(r"ufo:mtch:([a-z0-9\-]+)", event_id_raw)
                if match:
                    # Można przekonwertować na bigint, np. usuwając litery i myślniki:
                    bigint_candidate = re.sub(r"[^0-9]", "", match.group(1))
                    # event_id = int(bigint_candidate)
                    event_id = event_id_to_bigint(event_id_raw)

            time_tag = fixture.select_one("time")
            event_date_raw = time_tag.text.strip() if time_tag else None

            event_date_parsed = ""
            if event_date_raw:
                now = datetime.now()
                try:
                    if event_date_raw.startswith("dzisiaj"):
                        hour_minute = re.search(r"(\d{1,2}):(\d{2})", event_date_raw)
                        if hour_minute:
                            h, m = map(int, hour_minute.groups())
                            dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
                            event_date_parsed = dt.strftime("%Y%m%d%H%M")
                    elif event_date_raw.startswith("jutro"):
                        hour_minute = re.search(r"(\d{1,2}):(\d{2})", event_date_raw)
                        if hour_minute:
                            h, m = map(int, hour_minute.groups())
                            dt = now + timedelta(days=1)
                            dt = dt.replace(hour=h, minute=m, second=0, microsecond=0)
                            event_date_parsed = dt.strftime("%Y%m%d%H%M")
                    else:
                        # Usunięcie polskich skrótów dni tygodnia przed parsowaniem
                        cleaned_date = re.sub(r"^\w{3,6}\.,\s*", "", event_date_raw)
                        dt = datetime.strptime(cleaned_date, "%d.%m.%Y, %H:%M")
                        event_date_parsed = dt.strftime("%Y%m%d%H%M")
                except Exception as e:
                    event_date_parsed = f"PARSE_ERROR"

            event_date = event_date_parsed

            odds_test = fixture.find_all("span", class_=["odds-button__value-current", "f-font-bold", "f-text-xs"])
            print(f"len(odds_test) = {len(odds_test)}")

            teams = fixture.select(".fixture-card__participant-name")
            if len(teams) < 2:
                continue
            team1 = teams[0].get_text(strip=True)
            team2 = teams[1].get_text(strip=True)

            # Szukamy kursów dla zakładu "Wynik meczu

            markets = fixture.select(".fixture-card__market")
            if len(markets) < 1:
                markets = fixture.select(".fixture-card__markets")
            print(f"len markets = {len(markets)}")
            if not markets:
                markets = fixture.select(".fixture-card__markets")

            print(markets)
            match_result_odds = []
            for market in markets:
                market_name = market.select_one(".fixture-card__market-name")
                if market_name and market_name.get_text(strip=True).lower() == "wynik meczu":
                    outcomes = market.select(".fixture-card__market-odds")
                    for outcome in outcomes:
                        label = outcome.select_one(".odds-button__name").get_text(strip=True)
                        odd = outcome.select_one(".odds-button__value-current").get_text(strip=True)
                        match_result_odds.append((label, odd))
                        print(label)
                        print(odd)
                    break

            matches.append({
                # "event-id_raw" : event_id_raw,
                "market-name": team1 + "-" + team2,
                "event-id": event_id,
                # "Drużyna 1": team1,
                # "Drużyna 2": team2,
                "1": match_result_odds[0][1] if len(match_result_odds) > 0 else None,
                "X": match_result_odds[1][1] if len(match_result_odds) > 1 else None,
                "2": match_result_odds[2][1] if len(match_result_odds) > 2 else None,
                "event-datetime": event_date,
            })

        # Tworzymy DataFrame
        matches_df = pd.DataFrame(matches)
        print(matches_df)

        matches_df.to_csv('fortuna-test-extracted-data.csv', index=False)

        return matches_df

    except Exception as e:
        print("Nie udało się kliknąć:", e)
        return None


def extract_data_1(path=path):
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")
    response = requests.get(path, verify=False)
    if response.status_code != 200:
        print("Failed to fetch the page")
    else:
        print('connection to odd-provider website ok')

    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table', class_='table events-table')
    if table:
        competition_list = table.find_all('tr', class_='')
        print(f"events #: {len(competition_list)}")
    else:
        print("Nie znaleziono tabeli!")

    competition_list = soup.find('table', class_='table events-table').find_all('tr')
    print(f"events #: {len(competition_list)}")
    # print(competition_list[2])
    extracted_data_list = []

    for event in competition_list:
        market_name_elem = event.find('span', class_='market-name')
        if market_name_elem:
            row_data = []
            market_name_text = market_name_elem.get_text(strip=True)
            row_data.append(market_name_text)
            # Print or store the market name
            # print("Market Name:", market_name_text)

            event_id = event.find('a', class_='event-link js-event-link')['data-id']
            # print(event_id[3:])
            row_data.append(event_id[3:])

            odds_values = event.find_all('span', class_='odds-value')
            if odds_values:
                for odd in odds_values:
                    # Print or store the odds values
                    # print("Odds Value:", odd.get_text(strip=True))
                    row_data.append(odd.get_text(strip=True))

            date_value = event.find('span', class_='event-datetime')
            if date_value:
                # vprint(date_value.text)

                row_data.append(date_value.text)
            extracted_data_list.append(row_data)

    extracted_data_df = pd.DataFrame(extracted_data_list,
                                     columns=['market-name', 'event-id', '1', 'X', '2', 'event-datetime'])
    extracted_data_df.to_csv('fortuna-test-extracted-data.csv', index=False)
    return extracted_data_df


def scrapped_data():
    data = pd.read_csv(filepath_or_buffer='fortuna-test-extracted-data.csv')

    data['home'] = data['market-name'].str.split('-', n=1).str[0].str.strip()
    data['away'] = data['market-name'].str.split('-', n=1).str[1].str.strip()
    del data['market-name']
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d%H%M')
    data['update-date'] = formatted_date

    return data


def transform_data(teams, calendar):
    data = pd.read_csv(filepath_or_buffer='fortuna-test-extracted-data.csv')

    data['home'] = data['market-name'].str.split('-', n=1).str[0].str.strip()
    data['away'] = data['market-name'].str.split('-', n=1).str[1].str.strip()
    del data['market-name']
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d%H%M')
    data['update-date'] = formatted_date
    print(data)

    data.to_csv('fortuna-test-transformed-data.csv', index=False)
    # print(data.columns)

    # data = pd.merge(left=data, right=teams, how='left', left_on=['home'], right_on=['NAME_FORTUNA'])
    # data = pd.merge(left=data, right=teams, how='left', left_on=['away'], right_on=['NAME_FORTUNA'])
    # data = pd.merge(left=data, right=calendar, how='left', left_on=['event-datetime'], right_on=['DATE'])

    # check if new teams in home
    parameter = 'home'
    meta_parameter = 'away'

    test_home_join = pd.merge(left=data, right=teams, how='outer', left_on=[parameter], right_on=['NAME_FORTUNA'],
                              indicator=True)
    test_home_join = test_home_join[test_home_join['_merge'] == 'left_only']

    max_index_value = teams["ID"].max()
    new_teams = pd.DataFrame(test_home_join)
    new_teams.drop(
        ['event-id', '1', 'X', '2', 'event-datetime', meta_parameter, 'update-date', 'NAME_FORTUNA', '_merge'],
        axis='columns', inplace=True)

    new_teams["ID"] = max_index_value - 1 + new_teams.index
    new_teams.rename(columns={parameter: "NAME_FORTUNA"}, inplace=True)
    new_teams = pd.concat([teams, new_teams], ignore_index=False)

    teams = new_teams
    print(teams)

    # check if new teams in away
    test_home_join = pd.merge(left=data, right=teams, how='outer', left_on=['away'], right_on=['NAME_FORTUNA'],
                              indicator=True)
    test_home_join = test_home_join[test_home_join['_merge'] == 'left_only']

    max_index_value = new_teams["ID"].max()
    new_teams = pd.DataFrame(test_home_join)
    new_teams.drop(['event-id', '1', 'X', '2', 'event-datetime', 'home', 'update-date', 'NAME_FORTUNA', '_merge'],
                   axis='columns', inplace=True)

    new_teams["ID"] = max_index_value - 1 + new_teams.index
    new_teams.rename(columns={"away": "NAME_FORTUNA"}, inplace=True)
    new_teams = pd.concat([teams, new_teams], ignore_index=False)

    teams = new_teams
    print(teams)

    """
    #check null
    #print(data[data['ID_y'].isna()])
    #print(data[data['ID_x'].isna()])


    #data.drop(['event-datetime', 'home', 'away', 'NAME_FORTUNA_x', 'NAME_FORTUNA_y', 'DATE'],
    #          axis='columns', inplace=True)

    #data.to_csv('fortuna-test-transformed-data.csv', index=False)
    """
    # if new teams in away


def extract_data_from_database():
    engine = create_engine(f'postgresql+psycopg2://{s.user}:{s.password}@{s.host}:{s.port}/{s.dbname}')
    sql_query_teams = 'SELECT DISTINCT "ID", "NAME_FORTUNA" FROM teams'
    sql_query_calendar = 'SELECT DISTINCT "ID", "DATE" FROM calendar'

    teams = pd.read_sql(sql_query_teams, engine)
    calendar = pd.read_sql(sql_query_calendar, engine)

    print("extract_data_from_database completed")
    return teams, calendar


def update_table(df, table, mode):
    engine = create_engine(f'postgresql+psycopg2://{s.user}:{s.password}@{s.host}:{s.port}/{s.dbname}')
    df.to_sql(name=table, con=engine, if_exists=mode, index=False)

# SECTION ETL
# EXTRACT
extracted = extract_data()

print(f"extracted: {extracted}")

df_teams, df_calendar = extract_data_from_database()
df_from_scrap = scrapped_data()

# TRANSFORM
df_teams = check_if_new_teams_are_in_data(teams=df_teams, parameter='home')
df_teams = check_if_new_teams_are_in_data(teams=df_teams, parameter='away')
df_calendar = check_if_new_dates_in_data(calendar=df_calendar)

df_from_scrap = pd.merge(left=df_from_scrap, right=df_teams, how='left', left_on=['home'], right_on=['NAME_FORTUNA'])
df_from_scrap = pd.merge(left=df_from_scrap, right=df_teams, how='left', left_on=['away'], right_on=['NAME_FORTUNA'])
df_from_scrap = pd.merge(left=df_from_scrap, right=df_calendar, how='left', left_on=['event-datetime'],
                         right_on=['DATE'])

df_from_scrap.drop(['event-datetime', 'home', 'away', 'NAME_FORTUNA_x', 'NAME_FORTUNA_y', 'DATE'],
                   axis='columns', inplace=True)

df_from_scrap.rename(columns={"ID_x": "ID_team_home", "ID_y": "ID_team_away", "ID": "ID_date"}, inplace=True)
df_from_scrap.to_csv('fortuna-test-transformed-data.csv', index=False)

print("End of section: TRANSFORM")
# LOAD

update_table(df=df_teams, table='teams', mode='replace')
update_table(df=df_calendar, table='calendar', mode='replace')
update_table(df=df_from_scrap, table='odds', mode='append')
