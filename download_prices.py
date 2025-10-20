import requests
from datetime import datetime

def download_dayahead_prices():
    """Download APG Day Ahead prices from 2023 to today"""
    
    # Liste für alle Jahre
    years = [2023, 2024, 2025]
    all_csv_data = []
    
    for year in years:
        # Für aktuelles Jahr: bis heute, sonst ganzes Jahr
        if year == datetime.now().year:
            end_date = datetime.now().strftime("%Y-%m-%dT000000")
        else:
            end_date = f"{year}-12-31T000000"
        
        start_date = f"{year}-01-01T000000"
        
        csv_url = f"https://transparency.apg.at/api/v1/EXAAD1P/Download/de/PT15M/{start_date}/{end_date}"
        
        print(f"Downloading data for {year}...")
        response = requests.get(csv_url)
        
        if response.status_code == 200:
            all_csv_data.append(response.text)
            print(f"✓ Successfully downloaded {year}")
        else:
            print(f"✗ Error downloading {year}: {response.status_code}")
            return False
    
    # Header nur einmal verwenden
    combined_csv = all_csv_data[0]
    
    for csv_data in all_csv_data[1:]:
        # Alle Zeilen außer Header hinzufügen
        lines = csv_data.split('\n')
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
