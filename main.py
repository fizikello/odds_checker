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

TIME_SLEEP = 4
LEAGUE_PARAMETER = 5 # LEAGUE_ID

league_id = {1 : 'Ekstraklasa Polska',
             2: 'Polska 1.liga',
             3: 'Polska 2.',
             4: '1.Belgia',
             5: 'Polska 3.liga grupa II',
             6: 'Polska 3.liga grupa I',
             7: 'Polska 3.liga grupa III',
             8: 'Polska 3.liga grupa IV',
             9: '1.Anglia',
             10: '1.Niemcy'
            }

league_name = league_id[LEAGUE_PARAMETER]

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

def parse_event_date(event_date_raw: str,
                     now: datetime | None = None,
                     default_time: str = "00:00") -> str | None:

    if not event_date_raw:
        return None

    now = now or datetime.now()
    s = event_date_raw.strip().lower()
    s = s.replace('\xa0', ' ')      # NBSP → spacja
    s = s.replace('dziś', 'dzisiaj')  # unifikacja
    s = s.replace('godz.', '')      # usuń "godz." jeśli jest
    s = s.replace('godz', '')
    # usuń "o " (np. "dzisiaj o 17:00") ale nie usuwaj wszystkich 'o' w środku przypadkowych słów:
    s = re.sub(r'\bo\s+', '', s)

    def fmt(dt: datetime) -> str:
        return dt.strftime("%Y%m%d%H%M")

    # --- przypadki specjalne: "dzisiaj" / "jutro"
    if re.search(r'\bdzisiaj\b', s):
        tm = re.search(r'(\d{1,2})[:.](\d{2})', s)
        if tm:
            h, m = map(int, tm.groups())
        else:
            h, m = map(int, default_time.split(':'))
        dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
        return fmt(dt)

    if re.search(r'\bjutro\b', s):
        tm = re.search(r'(\d{1,2})[:.](\d{2})', s)
        if tm:
            h, m = map(int, tm.groups())
        else:
            h, m = map(int, default_time.split(':'))
        dt = (now + timedelta(days=1)).replace(hour=h, minute=m, second=0, microsecond=0)
        return fmt(dt)

    # --- szukamy: dd.mm(.yyyy) + czas HH:MM
    m = re.search(r'(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?.*?(\d{1,2})[:.](\d{2})', s)
    if m:
        day, month, year, hh, mm = m.groups()
        year = int(year) if year else now.year
        try:
            dt = datetime(year, int(month), int(day), int(hh), int(mm))
            return fmt(dt)
        except ValueError:
            return None

    # --- dd.mm(.yyyy) bez czasu -> użyj default_time
    m = re.search(r'(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?', s)
    if m:
        day, month, year = m.groups()
        year = int(year) if year else now.year
        h, mm = map(int, default_time.split(':'))
        try:
            dt = datetime(year, int(month), int(day), h, mm)
            return fmt(dt)
        except ValueError:
            return None

    # --- fallback: próbujemy kilka zwykłych formatów strptime (po usunięciu ewentualnych nazw dni na początku)
    cleaned = re.sub(r'^[^\d]*', '', s)  # usuń prefix (np. "pon., ")
    for fmt_try in ("%d.%m.%Y, %H:%M", "%d.%m.%Y %H:%M", "%d.%m.%Y",
                    "%d.%m, %H:%M", "%d.%m %H:%M", "%d.%m"):
        try:
            dt = datetime.strptime(cleaned, fmt_try)
            if "%Y" not in fmt_try:
                dt = dt.replace(year=now.year)
            if "%H" not in fmt_try:
                h, mm = map(int, default_time.split(':'))
                dt = dt.replace(hour=h, minute=mm)
            return fmt(dt)
        except Exception:
            continue

    # nic nie pasuje
    return None

def check_if_new_dates_in_data(calendar):
    # Upewnij się, że kluczowe kolumny mają ten sam typ danych
    df_from_scrap["event-datetime"] = df_from_scrap["event-datetime"].astype(str)
    calendar["DATE"] = calendar["DATE"].astype(str)

    df_unique = df_from_scrap.drop_duplicates(subset=['event-datetime'], keep='first')

    # Łączenie danych
    test_join = pd.merge(
        left=df_unique,
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
    #usuwam duplikaty daty

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

def process_one_card(soup_part):

    print(soup_part)
    for tag in soup_part.find_all('a'):
        print(tag.attrs)
    # print(soup_part.prettify()[:1000])
    tags = {tag.name for tag in soup_part.find_all(True)}
    print(tags)
    """
    features = []
    for a in soup_part.find_all('a', class_=lambda c: c and 'offer-fixture-card' in c):
        # data / czas
        time_tag = a.find('time')
        print(time_tag)
        date = time_tag.get_text(strip=True) if time_tag else None

        print(date)

        # drużyny
        participants = [p.get_text(" ", strip=True) for p in a.select('.fixture-card__participant')]
        home = participants[0] if len(participants) > 0 else None
        away = participants[1] if len(participants) > 1 else None

        # znajdź sekcję "Wynik meczu"
        market_section = None
        for sec in a.select('section.fixture-card__market'):
            heading = sec.find(lambda tag: tag.name == 'div' and 'Wynik meczu' in tag.get_text(" ", strip=True))
            if heading:
                market_section = sec
                break

        odds_1 = odds_x = odds_2 = None
        if market_section:
            outcomes = market_section.select('.odds-button2')
            pairs = []
            for o in outcomes:
                lbl_tag = o.select_one('.odds-button2__label')
                val_tag = o.select_one('.odds-button2__value')
                if lbl_tag and val_tag:
                    pairs.append((lbl_tag.get_text(strip=True), val_tag.get_text(strip=True)))

            # próbuj mapować etykiety (np. "Wisła K.", "Remis", "S.Rzeszów"), inaczej fallback po kolejności
            for idx, (lbl, val) in enumerate(pairs):
                if home and lbl.strip().lower() == home.strip().lower():
                    odds_1 = val
                elif 'remis' in lbl.strip().lower() or lbl.strip().lower() == 'x':
                    odds_x = val
                elif away and lbl.strip().lower() == away.strip().lower():
                    odds_2 = val
                else:
                    # fallback po kolejności: 0->1, 1->X, 2->2
                    if idx == 0 and odds_1 is None:
                        odds_1 = val
                    elif idx == 1 and odds_x is None:
                        odds_x = val
                    elif idx == 2 and odds_2 is None:
                        odds_2 = val

        features.append({
            "home": home,
            "away": away,
            "date": date,
            "odds_1": odds_1,
            "odds_X": odds_x,
            "odds_2": odds_2,
        })
        print(features)
"""


def extract_data_a():
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
        xpath = f"//div[contains(@class, 'side-menu-item__title') and contains(text(), '{league_name}')]"
        ekstraklasa = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((
            By.XPATH, xpath
            # By.XPATH, "//div[contains(@class, 'side-menu-item__title') and contains(text(), 'Ekstraklasa Polska')]"
            # By.XPATH, "//div[contains(@class, 'side-menu-item__title') and contains(text(), 'Polska 1.')]"
        )))
        driver.execute_script("arguments[0].scrollIntoView();", ekstraklasa)
        ekstraklasa.click()

        print(f"Kliknięto w {league_name}")
        time.sleep(TIME_SLEEP)

        #kursy_div = WebDriverWait(driver, 20).until(
        #    # EC.presence_of_element_located((By.CSS_SELECTOR, "span.odds-button__value-current"))
        #    EC.presence_of_element_located((By.CSS_SELECTOR, "grow offer-tournament-overview-container flex flex-col gap-8"))
        #)

        html = driver.page_source
        driver.close()


    except Exception as e:
        print("Nie udało się kliknąć:", e)
        return None

    try:
        with open("efortuna_ekstraklasa.html", "w", encoding="utf-8") as f:
            f.write(html)
            print('Download successful')

    except Exception as e:
        print('Saving failed')

        return  None

def extract_data_b():
    with open("efortuna_ekstraklasa.html", "r", encoding="utf-8") as f:
        html = f.read()

    # tworzymy obiekt BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    # Szukamy wszystkich kart meczowych
    fixtures_cards = soup.find_all("a", class_=lambda c: c and "offer-fixture-card" in c)

    matches = []
    for a_tag in fixtures_cards:
        #print(a_tag, end="\n")

        match_data = {}

        event_id_raw = a_tag.get("data-id")  # np. ufo:mtch:1kp-029
        event_id = None
        if event_id_raw:
            match = re.search(r"ufo:mtch:([a-z0-9\-]+)", event_id_raw)
            if match:
                bigint_candidate = re.sub(r"[^0-9]", "", match.group(1))
                event_id = event_id_to_bigint(event_id_raw)

        match_data["event-id"] = event_id

        ##  nazwa meczu (aria-label)
        #match_data["mecz"] = a_tag.get("aria-label", "")
        #print(match_data["mecz"])

        # nazwy drużyn (pierwsze dwa <div> z klasą fixture-card__participant)
        participants = a_tag.find_all("div", class_="fixture-card__participant")
        if len(participants) >= 2:
            #match_data["home"] = participants[0].get_text(strip=True)
            #match_data["away"] = participants[1].get_text(strip=True)
            match_data["market-name"] = participants[0].get_text(strip=True) + " - " + participants[1].get_text(strip=True)
        #print(match_data)

        # data meczu (w <time>)
        time_tag = a_tag.find("time")
        if time_tag:
            date_raw = time_tag.get_text(strip=True)
            event_date_parsed = parse_event_date(date_raw)
            match_data["event-datetime"] = event_date_parsed
            #print(match_data)
        # kursy — trzy pierwsze <div> z klasą odds-button2__value
        odds_tags = a_tag.find_all("div", class_="odds-button2__value")
        if len(odds_tags) >= 3:
            match_data["1"] = odds_tags[0].get_text(strip=True)
            match_data["X"] = odds_tags[1].get_text(strip=True)
            match_data["2"] = odds_tags[2].get_text(strip=True)
        else:
            # brak kursów (np. mecz w przygotowaniu)
            match_data["1"] = match_data["X"] = match_data["2"] = None
        print(match_data)
        matches.append(match_data)

    matches_df = pd.DataFrame(matches)
    matches_df.to_csv('fortuna-test-extracted-data.csv', index=False)

    return matches_df


    # for a_tag in soup.find_all("a", class_=lambda c: c and "offer-fixture-card" in c):
    #    print(f"a_tag: {a_tag}")

    # fixtures = soup.find_all("a", class_="no-underline fixture-safe-link cursor-pointer fixture-card w-full last:rounded-b-lg offer-fixture-card relative")
    # print(f"fixtures: {len(fixtures)}")
    # print(fixtures[1])


def extract_data():
    driver = webdriver.Chrome()
    driver.maximize_window()

    driver.get("https://www.efortuna.pl/")
    time.sleep(5)

    driver.execute_script("""
    var overlay = document.getElementById('cookie-consent-overlay');
    if(overlay) {
      overlay.style.display = 'none';
    }
    """)

    time.sleep(1)

    try:
        xpath = f"//div[contains(@class, 'side-menu-item__title') and contains(text(), '{league_name}')]"
        ekstraklasa = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((
            By.XPATH, xpath
            # By.XPATH, "//div[contains(@class, 'side-menu-item__title') and contains(text(), 'Ekstraklasa Polska')]"
            # By.XPATH, "//div[contains(@class, 'side-menu-item__title') and contains(text(), 'Polska 1.')]"
        )))
        driver.execute_script("arguments[0].scrollIntoView();", ekstraklasa)
        ekstraklasa.click()

        print(f"Kliknięto w {league_name}")
        time.sleep(TIME_SLEEP)

        #kursy_div = WebDriverWait(driver, 20).until(
        #    # EC.presence_of_element_located((By.CSS_SELECTOR, "span.odds-button__value-current"))
        #    EC.presence_of_element_located((By.CSS_SELECTOR, "grow offer-tournament-overview-container flex flex-col gap-8"))
        #)

        html = driver.page_source
        with open("efortuna_ekstraklasa.html", "w", encoding="utf-8") as f:
            f.write(html)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()

        # Szukamy wszystkich kart meczowych
        fixtures = soup.find_all("a", class_="fixture-safe-link")
        print(f"fixtures: {len(fixtures)}")
        matches = []

        ###
        for fixture in fixtures:
            event_id_raw = fixture.get("data-id")  # np. ufo:mtch:1kp-029
            event_id = None
            if event_id_raw:
                match = re.search(r"ufo:mtch:([a-z0-9\-]+)", event_id_raw)
                if match:
                    bigint_candidate = re.sub(r"[^0-9]", "", match.group(1))
                    event_id = event_id_to_bigint(event_id_raw)

            time_tag = fixture.select_one("time")
            event_date_raw = time_tag.text.strip() if time_tag else None
            event_date_parsed = parse_event_date(event_date_raw)
            event_date = event_date_parsed
            print(f"parsed: {event_date_parsed}")

            # Pobieramy drużyny
            teams = fixture.select(".fixture-card__participant-name")
            print(f"teams: {teams}")
            if len(teams) < 2:
                continue
            team1 = teams[0].get_text(strip=True)
            team2 = teams[1].get_text(strip=True)

            # Pobieramy kursy dla rynku "Wynik meczu"
            match_result_odds = []
            market = fixture.select_one(".fixture-card__market-outcomes")
            if market:
                outcomes = market.select(".fixture-card__market-odds")
                for outcome in outcomes:
                    label = outcome.select_one(".odds-button__name")
                    odd = outcome.select_one(".odds-button__value-current")
                    if label and odd:
                        match_result_odds.append((label.get_text(strip=True), odd.get_text(strip=True)))
                        print(label.get_text(strip=True), odd.get_text(strip=True))

            # fallback → jeśli brak kursów, to pola będą None
            matches.append({
                "market-name": f"{team1} - {team2}",
                "event-id": event_id,
                "1": match_result_odds[0][1] if len(match_result_odds) > 0 else None,
                "X": match_result_odds[1][1] if len(match_result_odds) > 1 else None,
                "2": match_result_odds[2][1] if len(match_result_odds) > 2 else None,
                "event-datetime": event_date,
            })

        # Tworzymy DataFrame
        matches_df = pd.DataFrame(matches)
        print(matches_df)


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
extract_data_a()
# exctract_data_a()
extracted = extract_data_b()
print(extracted)

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

df_from_scrap["ID_LEAGUE"] = LEAGUE_PARAMETER
df_from_scrap.to_csv('fortuna-test-transformed-data.csv', index=False)
print(df_from_scrap)
print("End of section: TRANSFORM")
# LOAD
update_table(df=df_teams, table='teams', mode='replace')
update_table(df=df_calendar, table='calendar', mode='replace')
update_table(df=df_from_scrap, table='odds', mode='append')
print("End of section: LOAD")