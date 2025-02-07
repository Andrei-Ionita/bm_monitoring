from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

def get_latest_balancing_data():
    # Initialize WebDriver
    driver = webdriver.Chrome()
    url = "https://newmarkets.transelectrica.ro/uu-webkit-maing02/00121011300000000000000000000100/activatedBalancingEnergyOverview"

    try:
        driver.get(url)

        # Accept cookies if present
        try:
            WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'I understand')]"))
            ).click()
            print("Cookie accepted.")
        except:
            print("No cookie popup detected.")

        # Wait for the latest row to be present
        latest_row = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "uutils-1p7r1e8"))
        )

        # Extract all divs inside the row
        cells = latest_row.find_elements(By.TAG_NAME, "div")
        if len(cells) >= 5:
            # Extract the relevant data
            time_period = cells[0].text.strip()
            afrr_up = float(cells[1].text.strip() or 0)
            afrr_down = float(cells[2].text.strip() or 0)
            mfrr_up = float(cells[3].text.strip() or 0)
            mfrr_down = float(cells[4].text.strip() or 0)

            # Return as a single row DataFrame for simplicity
            return pd.DataFrame([[time_period, afrr_up, afrr_down, mfrr_up, mfrr_down]],
                                columns=["Time Period", "aFRR Up (MWh)", "aFRR Down (MWh)", "mFRR Up (MWh)", "mFRR Down (MWh)"])
        else:
            print("No data found in the latest row.")
            return pd.DataFrame()  # Return empty DataFrame if no data

    finally:
        driver.quit()

# Test the function
if __name__ == "__main__":
    df = get_latest_balancing_data()
    print(df)
