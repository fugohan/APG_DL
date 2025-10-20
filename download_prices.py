import requests
from datetime import datetime
import time

def download_dayahead_prices():
    """Download APG Day Ahead prices from 2023 to today with retry logic"""
    
    years = [2023, 2024, 2025]
    all_csv_data = []
    
    # Session mit Timeout-Konfiguration
    session = requests.Session()
    session.timeout = (10, 120)
    
    for year in years:
        # Für aktuelles Jahr: bis heute, sonst ganzes Jahr
        if year == datetime.now().year:
            end_date = datetime.now().strftime("%Y-%m-%dT000000")
        else:
            end_date = f"{year}-12-31T000000"
        
        start_date = f"{year}-01-01T000000"
        csv_url = f"https://transparency.apg.at/api/v1/EXAAD1P/Download/de/PT15M/{start_date}/{end_date}"
        
        print(f"Downloading data for {year}...")
        
        # Retry-Logik
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = session.get(csv_url, timeout=(10, 120))
                
                if response.status_code == 200:
                    all_csv_data.append(response.text)
                    print(f"✓ Successfully downloaded {year}")
                    break
                elif response.status_code == 429:
                    wait_time = int(response.headers.get('Retry-After', 60))
                    print(f"Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"✗ Error {response.status_code} for {year}")
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        print(f"Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        return False
                        
            except requests.Timeout:
                print(f"⚠ Timeout for {year} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    wait_time = 5 * (2 ** attempt)
                    print(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"✗ Failed to download {year} after {max_retries} attempts")
                    return False
                    
            except requests.RequestException as e:
                print(f"✗ Request error for {year}: {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = 5 * (2 ** attempt)
                    print(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    return False
    
    if not all_csv_data:
        print("✗ No data downloaded")
        return False
    
    # Custom Header verwenden
    custom_header = "Zeit von [CET/CEST];Zeit bis [CET/CEST];Preis MC Auktion [EUR/MWh];MC Referenzpreis [EUR/MWh]"
    
    # Ersten Datensatz verarbeiten - alten Header durch neuen ersetzen
    lines = all_csv_data[0].split('\n')
    combined_csv = custom_header + '\n' + '\n'.join(lines[1:])
    
    # Rest der Daten anhängen (ohne Header)
    for csv_data in all_csv_data[1:]:
        lines = csv_data.split('\n')
        if len(lines) > 1:
            combined_csv += '\n'.join(lines[1:])
    
    # In Datei speichern
    filename = 'dayahead_prices_2023-2025.csv'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(combined_csv)
    
    print(f"✓ Data saved to {filename}")
    return True

if __name__ == "__main__":
    success = download_dayahead_prices()
    exit(0 if success else 1)
