from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import re

TIME_SLEEP = 8

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
    #html = kursy_div.get_attribute('outerHTML')

    #with open("test_page.html", "w", encoding="utf-8") as f:
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
                #event_id = int(bigint_candidate)
                event_id = event_id_to_bigint(event_id_raw)

        time_tag = fixture.select_one("time")
        event_date = time_tag.text.strip() if time_tag else None

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
            #"event-id_raw" : event_id_raw,
            "market-name": team1 + "-" + team2,
            "event-id" : event_id,
            #"Drużyna 1": team1,
            #"Drużyna 2": team2,
            "1": match_result_odds[0][1] if len(match_result_odds) > 0 else None,
            "X": match_result_odds[1][1] if len(match_result_odds) > 1 else None,
            "2": match_result_odds[2][1] if len(match_result_odds) > 2 else None,
            "event-datetime" : event_date,
        })

    # Tworzymy DataFrame
    matches_df = pd.DataFrame(matches)
    print(matches_df)

    matches_df.to_csv("fortuna_01.csv", index=False)


except Exception as e:
    print("Nie udało się kliknąć:", e)

driver.quit() # gdy chcesz zakończyć sesję