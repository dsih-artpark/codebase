
import pandas as pd
from googlemaps import Client as GoogleMaps
import googlemaps
import gmaps
from tqdm import tqdm


# # Run this if you have encrypted the API key locally
# import subprocess

# # Enter the path to your openssl encoded file
# encrypted_file = "~/config.enc"

# # Command to decrypt using OpenSSL - You will be prompted to enter the openssl password used for encryption
# command = f"openssl aes-256-cbc -d -salt -in {encrypted_file}"

# try:
#     MyAPI=subprocess.check_output(command, shell=True, text=True).strip()
# except subprocess.CalledProcessError as e:
#     raise(e)

# Geocoding function

def geocode(full_address: str, API_key: str) -> tuple:
    """_summary_

    Args:
        full_address (str): concatenated address to include all relevant geographical fields

    Returns:
        tuple: lat, long
    """
    if pd.isna(full_address):
        return pd.NA
    else:
        assert isinstance(full_address, str) and isinstance(
            API_key, str) and len(API_key) == 39, "invalid input"
        gmaps = googlemaps.Client(key=API_key)
        try:
            geocode_result = gmaps.geocode(full_address)
            if geocode_result:
                lat = geocode_result[0]['geometry']['location']['lat']
                long = geocode_result[0]['geometry']['location']['lng']
                return lat, long
            else:
                raise Exception("No result returned.")
        except Exception as e:
            print(f"Geocoding failed {e}")
            return None, None

# sample implementation with progress bar
# for idx, row in tqdm(df.iterrows(), total=df.shape[0]):
#     # change padding as required
#     address = row['metadata.nameAddress'] + row['metadata.admin5.name'] + row['metadata.admin4.name'] + row['metadata.admin3.name'] + row['metadata.admin2.name']
#     if isinstance(address, str) and address:
#         row['location.geometry.latitude'], row['location.geometry.longitude'] = geocode(full_address=address, API_key=MyAPI)
