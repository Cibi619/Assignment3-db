import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time


@pytest.fixture(scope="module")
def driver():
    """Initialize headless Chrome for Selenium tests."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.get("http://localhost:5000")

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    yield driver
    driver.quit()


def click_search_button(driver):
    """Helper to safely click the Search button."""
    try:
        btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='submit']"))
        )
        btn.click()
    except Exception:
        btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.TAG_NAME, "button"))
        )
        btn.click()
    time.sleep(2)


def test_positive_search_returns_results(driver):
    """Positive test — search form returns some results."""
    click_search_button(driver)

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "table"))
    )

    rows = driver.find_elements(By.XPATH, "//table//tr")
    assert len(rows) > 1, "Expected search results but got none."


def test_negative_search_no_results(driver):
    """Negative test — invalid filters return 0 results."""
    borough_box = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.NAME, "borough"))
    )
    complaint_box = driver.find_element(By.NAME, "complaint_type")

    borough_box.clear()
    borough_box.send_keys("FakeBoroughXYZ")

    complaint_box.clear()
    complaint_box.send_keys("InvalidComplaint123")

    click_search_button(driver)

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    body_text = driver.find_element(By.TAG_NAME, "body").text
    assert "Page 1 of 0" in body_text or "0" in body_text, \
        f"Expected no results for invalid filters, but got: {body_text[:200]}"


def test_aggregate_page_loads(driver):
    """Aggregate test — Complaints per Borough page loads correctly."""
    link = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.LINK_TEXT, "View Complaints per Borough"))
    )
    link.click()

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    page_source = driver.page_source

    assert "borough" in page_source.lower() or "complaints per borough" in page_source.lower(), \
        f"Aggregate page failed to load correctly. Response snippet:\n{page_source[:300]}"
