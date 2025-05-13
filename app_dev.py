import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
import threading
from playsound import playsound
import time
import base64
from twilio.rest import Client
from dotenv import load_dotenv
import os
import asyncio
import openai
import json
from dotenv import load_dotenv
from base64 import b64encode
from openai import OpenAI
import zipfile
import xml.etree.ElementTree as ET
import re

load_dotenv()
api_key_entsoe = os.getenv("API_KEY_ENTSOE")

# Set the EET timezone
eet_timezone = pytz.timezone('Europe/Bucharest')

# Page configuration for wide layout
st.set_page_config(layout="wide")

# Initialize session state for separate alarm tracking and call history
if "last_warning_alarms" not in st.session_state:
    st.session_state["last_warning_alarms"] = []

if "last_critical_alarms" not in st.session_state:
    st.session_state["last_critical_alarms"] = []

if "calls_made_critical" not in st.session_state:
    st.session_state["calls_made_critical"] = []

if "calls_made_warning" not in st.session_state:
    st.session_state["calls_made_warning"] = []

if "last_alarm_check_time" not in st.session_state:
    st.session_state["last_alarm_check_time"] = datetime.now(pytz.timezone('Europe/Bucharest'))

if "processed_alarms" not in st.session_state:
    st.session_state["processed_alarms"] = set()  # Store alarms that were already called

# Ensure all alarms persist across updates
if "all_alarms" not in st.session_state:
    st.session_state["all_alarms"] = []

# Initialize session state for the phone number
if "user_phone_number" not in st.session_state:
    st.session_state["user_phone_number"] = ""  # Default number

# Default thresholds
default_thresholds = {
    "THRESHOLD_AFRR_UP": 20,
    "THRESHOLD_AFRR_DOWN": 15,
    "THRESHOLD_MFRR_UP": 30,
    "THRESHOLD_MFRR_DOWN": 25,
    "RATE_OF_CHANGE_THRESHOLD": 20,
    "AFRR_SPIKE_THRESHOLD": 25
}

# Sidebar inputs for adjustable thresholds
st.sidebar.header("Adjust Alarm Thresholds (Leave blank to use defaults)")
THRESHOLD_AFRR_UP = int(st.sidebar.text_input("Threshold aFRR Up (MWh)", default_thresholds["THRESHOLD_AFRR_UP"]) or default_thresholds["THRESHOLD_AFRR_UP"])
THRESHOLD_AFRR_DOWN = int(st.sidebar.text_input("Threshold aFRR Down (MWh)", default_thresholds["THRESHOLD_AFRR_DOWN"]) or default_thresholds["THRESHOLD_AFRR_DOWN"])
THRESHOLD_MFRR_UP = int(st.sidebar.text_input("Threshold mFRR Up (MWh)", default_thresholds["THRESHOLD_MFRR_UP"]) or default_thresholds["THRESHOLD_MFRR_UP"])
THRESHOLD_MFRR_DOWN = int(st.sidebar.text_input("Threshold mFRR Down (MWh)", default_thresholds["THRESHOLD_MFRR_DOWN"]) or default_thresholds["THRESHOLD_MFRR_DOWN"])
RATE_OF_CHANGE_THRESHOLD = int(st.sidebar.text_input("Rate of Change Threshold (MWh)", default_thresholds["RATE_OF_CHANGE_THRESHOLD"]) or default_thresholds["RATE_OF_CHANGE_THRESHOLD"])
AFRR_SPIKE_THRESHOLD = int(st.sidebar.text_input("aFRR Spike Threshold (MWh)", default_thresholds["AFRR_SPIKE_THRESHOLD"]) or default_thresholds["AFRR_SPIKE_THRESHOLD"])
# Button to clear processed alarms (for debugging)
if st.sidebar.button("Reset Processed Alarms"):
    st.session_state["processed_alarms"] = set()
    st.session_state["last_alarm_check_time"] = datetime.now(pytz.timezone('Europe/Bucharest'))
    st.sidebar.success("Processed alarms cleared.")
# Sidebar input linked to session state
USER_PHONE_NUMBER = st.sidebar.text_input(
    "Enter Phone Number for Alerts (with country code)",
    value=st.session_state["user_phone_number"],  
    placeholder="+407XXXXXXXX"
).strip()  # Remove extra spaces

# Validate the phone number input
def is_valid_phone_number(phone_number):
    return phone_number.startswith("+") and len(phone_number) > 9

if not is_valid_phone_number(USER_PHONE_NUMBER):
    st.sidebar.error("Please enter a valid phone number with the country code.")

# Store the updated phone number in session state
st.session_state["user_phone_number"] = USER_PHONE_NUMBER if is_valid_phone_number(USER_PHONE_NUMBER) else st.session_state["user_phone_number"]


# Load environment variables from .env file
load_dotenv()

# Twilio account configuration
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")

# Initialize Twilio client
client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# Function to check if current time is within night hours (12 AM to 8 AM EET)
def is_night_time():
    eet_timezone = pytz.timezone('Europe/Bucharest')
    current_time_eet = datetime.now(eet_timezone).time()
    return current_time_eet >= datetime.strptime("00:00", "%H:%M").time() and current_time_eet <= datetime.strptime("08:00", "%H:%M").time()

# Function to make a phone call for alarms
def make_call(alarm_type, alarm_message, alarm_id):
    """Make a phone call for critical alarms only if the alarm is new."""
    to_phone = st.session_state["user_phone_number"]
    
    if not is_valid_phone_number(to_phone):
        print(f"Invalid phone number: {to_phone}. Skipping call.")
        return

    # Check if the alarm was already processed
    if alarm_id in st.session_state["processed_alarms"]:
        print(f"Skipping already processed alarm: {alarm_message}")
        return

    try:
        current_time_eet = datetime.now(pytz.timezone('Europe/Bucharest')).strftime("%Y-%m-%d %H:%M:%S")

        call = client.calls.create(
            twiml=f'<Response><Say>{alarm_type} alarm: {alarm_message} detected at {current_time_eet}. Please check the system immediately.</Say></Response>',
            to=to_phone,
            from_=TWILIO_PHONE_NUMBER
        )
        
        print(f"{alarm_type} call initiated successfully! Call SID: {call.sid}")

        # Mark this alarm as processed
        st.session_state["processed_alarms"].add(alarm_id)

    except Exception as e:
        print(f"Error making the call: {e}")

# Function to fetch and convert data to EET
def fetch_balancing_energy_data():
    """Fetch activated balancing energy data, convert timestamps to EET, and filter only today's data."""

    # Define timezone
    eet_timezone = pytz.timezone('Europe/Bucharest')
    
    # Get current date in **EET** and set midnight as start of the day
    eet_now = datetime.now(eet_timezone)
    eet_midnight = eet_now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Convert midnight EET to UTC (since API operates in UTC)
    utc_midnight = eet_midnight.astimezone(pytz.utc)

    # Set API time range: Fetch from **EET midnight (converted to UTC) until now**
    from_time = utc_midnight.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to_time = (utc_midnight + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Debug: Print time range used for fetching data
    print(f"Fetching data from {from_time} to {to_time} (UTC)")

    # API Request
    url = f"https://newmarkets.transelectrica.ro/usy-durom-publicreportg01/00121002500000000000000000000100/publicReport/activatedBalancingEnergyOverview?timeInterval.from={from_time}&timeInterval.to={to_time}&pageInfo.pageSize=3000"
    
    response = requests.get(url)
    if response.status_code != 200:
        st.error(f"Failed to fetch data. Status code: {response.status_code}")
        return pd.DataFrame()

    # Parse JSON response
    data = response.json()
    items = data.get("itemList", [])

    # Debug: Print number of fetched records
    print(f"Fetched {len(items)} records from API.")

    # Process and convert timestamps
    rows = []
    for item in items:
        try:
            # Convert timestamps from UTC to EET
            utc_from = datetime.fromisoformat(item['timeInterval']['from'].replace('Z', '+00:00'))
            utc_to = datetime.fromisoformat(item['timeInterval']['to'].replace('Z', '+00:00'))

            eet_from = utc_from.astimezone(eet_timezone)
            eet_to = utc_to.astimezone(eet_timezone)

            # Debugging - Print all fetched rows
            print(f"Processing: {eet_from} - {eet_to}")

            # Filter out rows **before today's midnight (EET)**
            if eet_from < eet_midnight:
                print(f"Skipping {eet_from} - before today's midnight")
                continue  # Skip records from the previous day

            # Store in formatted string
            time_period = f"{eet_from.strftime('%Y-%m-%d %H:%M:%S')} - {eet_to.strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Extract energy values (default to 0 if missing)
            afrr_up = item.get("aFRR_Up", 0) or 0
            afrr_down = item.get("aFRR_Down", 0) or 0
            mfrr_up = item.get("mFRR_Up", 0) or 0
            mfrr_down = item.get("mFRR_Down", 0) or 0

            # Debugging - Log all added records
            print(f"ADDING: {time_period} | aFRR_Up: {afrr_up}, aFRR_Down: {afrr_down}, mFRR_Up: {mfrr_up}, mFRR_Down: {mfrr_down}")

            # Store processed row
            rows.append([time_period, afrr_up, afrr_down, mfrr_up, mfrr_down])

        except Exception as e:
            print(f"Error processing record: {e}")

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=["Time Period (EET)", "aFRR Up (MWh)", "aFRR Down (MWh)", "mFRR Up (MWh)", "mFRR Down (MWh)"])
    
    # Debug: Print first few rows of the dataframe
    print("Processed DataFrame:")
    print(df.head())

    return df

def fetch_marginal_prices():
    """
    Fetch marginal activation prices for balancing energy.
    Combines mFRR Scheduled and Direct into one per direction.
    """
    import requests
    from datetime import datetime, timedelta
    import pytz
    import pandas as pd

    eet = pytz.timezone("Europe/Bucharest")
    now_eet = datetime.now(eet)
    midnight_eet = now_eet.replace(hour=0, minute=0, second=0, microsecond=0)

    from_time_utc = midnight_eet.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to_time_utc = (midnight_eet + timedelta(days=1)).astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    url = f"https://newmarkets.transelectrica.ro/usy-durom-publicreportg01/00121002500000000000000000000100/publicReport/marginalPricesOverview?timeInterval.from={from_time_utc}&timeInterval.to={to_time_utc}&pageInfo.pageSize=3000"

    response = requests.get(url)
    if response.status_code != 200:
        st.error(f"Failed to fetch marginal prices. Status code: {response.status_code}")
        return pd.DataFrame()

    data = response.json().get("itemList", [])

    processed = []
    for item in data:
        try:
            utc_from = datetime.fromisoformat(item["timeInterval"]["from"].replace("Z", "+00:00"))
            utc_to = datetime.fromisoformat(item["timeInterval"]["to"].replace("Z", "+00:00"))

            eet_from = utc_from.astimezone(eet)
            eet_to = utc_to.astimezone(eet)

            time_period = f"{eet_from.strftime('%Y-%m-%d %H:%M:%S')} - {eet_to.strftime('%Y-%m-%d %H:%M:%S')}"

            aFRR_up = item.get("aFRR_Up", 0) or 0
            aFRR_down = item.get("aFRR_Down", 0) or 0

            mFRR_up_scheduled = item.get("mFRR_Up_Scheduled", 0) or 0
            mFRR_up_direct = item.get("mFRR_Up_Direct", 0) or 0
            mFRR_down_scheduled = item.get("mFRR_Down_Scheduled", 0) or 0
            mFRR_down_direct = item.get("mFRR_Down_Direct", 0) or 0

            mFRR_up_total = mFRR_up_scheduled + mFRR_up_direct
            mFRR_down_total = mFRR_down_scheduled + mFRR_down_direct

            processed.append([
                time_period,
                aFRR_up,
                aFRR_down,
                mFRR_up_total,
                mFRR_down_total
            ])
        except Exception as e:
            print(f"Error parsing marginal price row: {e}")
            continue

    df = pd.DataFrame(processed, columns=[
        "Time Period (EET)",
        "aFRR Up Price (RON/MWh)",
        "aFRR Down Price (RON/MWh)",
        "mFRR Up Price (RON/MWh)",
        "mFRR Down Price (RON/MWh)"
    ])

    return df

# Fetching the Imbalance volumes========================================================
def create_combined_imbalance_dataframe(df_prices, df_volumes):
	"""
	This function combines the imbalance prices and volumes into a single DataFrame.
	
	Parameters:
	df_prices (DataFrame): DataFrame containing timestamps, Excedent Price, and Deficit Price.
	df_volumes (DataFrame): DataFrame containing timestamps and Imbalance Volume.

	Returns:
	DataFrame: Combined DataFrame with Timestamp, Excedent Price, Deficit Price, and Imbalance Volume.
	"""
	# Merge prices and volumes on the Timestamp column using an outer join to ensure all data is included.
	df_combined = pd.merge(df_prices, df_volumes, on='Timestamp', how='outer')

	# Sort the combined DataFrame by Timestamp to keep it in chronological order.
	df_combined = df_combined.sort_values(by='Timestamp')

	# Fill any missing values with 0.0 to ensure consistency in analysis.
	df_combined.fillna(0.0, inplace=True)

	return df_combined

def fetch_intraday_imbalance_volumes():
	"""Fetch today's estimated system imbalance volumes from Transelectrica DAMAS API in EET."""

	# Define timezone (Europe/Bucharest = EET)
	eet_timezone = pytz.timezone("Europe/Bucharest")

	# Get current time and midnight in EET
	eet_now = datetime.now(eet_timezone)
	eet_midnight = eet_now.replace(hour=0, minute=0, second=0, microsecond=0)

	# Convert EET timestamps to UTC for the API call
	utc_midnight = eet_midnight.astimezone(pytz.utc)
	utc_now = eet_now.astimezone(pytz.utc)

	# Format time strings
	from_time = utc_midnight.strftime("%Y-%m-%dT%H:%M:%S.000Z")
	to_time = utc_now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

	# API request to the same DAMAS endpoint
	url = (
		"https://newmarkets.transelectrica.ro/usy-durom-publicreportg01/"
		"00121002500000000000000000000100/publicReport/estimatedPowerSystemImbalance"
		f"?timeInterval.from={from_time}&timeInterval.to={to_time}&pageInfo.pageSize=3000"
	)

	try:
		response = requests.get(url)
		if response.status_code != 200:
			print(f"❌ Failed to fetch data for imbalance volumes. Status code: {response.status_code}")
			return pd.DataFrame(columns=["Timestamp", "Imbalance Volume"])

		# Parse the JSON response
		data = response.json()
		items = data.get("itemList", [])

		print(f"✅ Successfully fetched {len(items)} records for Imbalance Volume from API.")

		rows = []
		for item in items:
			try:
				# Convert timestamp
				utc_from = datetime.fromisoformat(item['timeInterval']['from'].replace('Z', '+00:00'))
				eet_from = utc_from.astimezone(eet_timezone)

				# Extract the imbalance volume (can be positive or negative)
				imbalance_volume = float(item.get("estimatedSystemImbalance", 0) or 0)

				# Shift to delivery interval
				rows.append([eet_from + timedelta(minutes=15), imbalance_volume])
			except Exception as item_error:
				print(f"Error processing item for imbalance volume: {item_error}")

		if not rows:
			print("⚠️ No valid imbalance volume rows processed.")
			return pd.DataFrame(columns=["Timestamp", "Imbalance Volume"])

		df = pd.DataFrame(rows, columns=["Timestamp", "Imbalance Volume"])
		df["Timestamp"] = pd.to_datetime(df["Timestamp"]).dt.tz_localize(None)

		return df

	except requests.exceptions.RequestException as req_error:
		print(f"❌ Request failed for imbalance volume data: {req_error}")
		return pd.DataFrame(columns=["Timestamp", "Imbalance Volume"])
	except Exception as general_error:
		print(f"❌ Error processing imbalance volume response: {general_error}")
		return pd.DataFrame(columns=["Timestamp", "Imbalance Volume"])
def fetch_intraday_imbalance_prices():
	"""Fetch today's estimated imbalance prices (positive/negative) from DAMAS in EET."""
	# Timezone for local EET
	eet = pytz.timezone("Europe/Bucharest")

	# Get EET midnight and now
	eet_now = datetime.now(eet)
	eet_midnight = eet_now.replace(hour=0, minute=0, second=0, microsecond=0)

	# Convert to UTC for API
	utc_start = eet_midnight.astimezone(pytz.utc)
	utc_end = eet_now.astimezone(pytz.utc)

	from_time = utc_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
	to_time = utc_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

	url = (
		"https://newmarkets.transelectrica.ro/usy-durom-publicreportg01/"
		"00121002500000000000000000000100/publicReport/estimatedImbalancePrices"
		f"?timeInterval.from={from_time}&timeInterval.to={to_time}&pageInfo.pageSize=3000"
	)

	try:
		response = requests.get(url)
		if response.status_code != 200:
			print(f"❌ Failed to fetch imbalance prices. HTTP {response.status_code}")
			return pd.DataFrame(columns=["Timestamp", "Imbalance Price Positive", "Imbalance Price Negative"])

		data = response.json()
		items = data.get("itemList", [])

		print(f"✅ Retrieved {len(items)} imbalance price records.")

		rows = []
		for item in items:
			try:
				utc_ts = datetime.fromisoformat(item["timeInterval"]["from"].replace("Z", "+00:00"))
				eet_ts = utc_ts.astimezone(eet) + timedelta(minutes=15)

				price_pos = float(item.get("estimatedPricePositiveImbalance", 0) or 0)
				price_neg = float(item.get("estimatedPriceNegativeImbalance", 0) or 0)

				rows.append([eet_ts, price_pos, price_neg])
			except Exception as e:
				print(f"⚠️ Error parsing price item: {e}")

		if not rows:
			print("⚠️ No valid imbalance price data processed.")
			return pd.DataFrame(columns=["Timestamp", "Imbalance Price Positive", "Imbalance Price Negative"])

		df = pd.DataFrame(rows, columns=["Timestamp", "Imbalance Price Positive", "Imbalance Price Negative"])
		df["Timestamp"] = pd.to_datetime(df["Timestamp"]).dt.tz_localize(None)
		df.sort_values("Timestamp", inplace=True)
		df.reset_index(drop=True, inplace=True)

		return df

	except Exception as e:
		print(f"❌ Error during imbalance price fetch: {e}")
		return pd.DataFrame(columns=["Timestamp", "Imbalance Price Positive", "Imbalance Price Negative"])

# IGCC===========================================================================================
def fetch_igcc_netting_flows():
	"""Fetch real-time activated balancing energy data from Transelectrica API for today in CET.
	Note: The API endpoint used seems to be for general activated balancing energy,
	not specifically IGCC netting. The field names 'imbalanceNettingImport' and
	'imbalanceNettingExport' are used, but might return 0 if not present in the response.
	"""

	# Define CET timezone
	cet_timezone = pytz.timezone("Europe/Bucharest")

	# Get current time in CET and set midnight as start of the day
	cet_now = datetime.now(cet_timezone)
	cet_midnight = cet_now.replace(hour=0, minute=0, second=0, microsecond=0)

	# Convert CET midnight to UTC (Transelectrica API operates in UTC)
	utc_midnight = cet_midnight.astimezone(pytz.utc)
	utc_now = cet_now.astimezone(pytz.utc)

	# Set API time range: Fetch from CET midnight (converted to UTC) until now
	from_time = utc_midnight.strftime("%Y-%m-%dT%H:%M:%S.000Z")
	to_time = utc_now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

	# API Request (Transelectrica)
	# This URL fetches activatedBalancingEnergyOverview
	url = f"https://newmarkets.transelectrica.ro/usy-durom-publicreportg01/00121002500000000000000000000100/publicReport/estimatedPowerSystemImbalance?timeInterval.from={from_time}&timeInterval.to={to_time}&pageInfo.pageSize=3000"

	try:
		response = requests.get(url)
		# Check response status
		if response.status_code != 200:
			print(f"❌ Failed to fetch data for IGCC flows. Status code: {response.status_code}")
			return pd.DataFrame(columns=["Timestamp", "IGCC Import (MW)", "IGCC Export (MW)"])

		# Parse JSON response
		data = response.json()
		items = data.get("itemList", [])

		print(f"✅ Successfully fetched {len(items)} records for IGCC processing from API.")

		# Process and convert timestamps
		rows = []
		for item in items:
			try:
				# Convert timestamps from UTC to CET
				utc_from = datetime.fromisoformat(item['timeInterval']['from'].replace('Z', '+00:00'))
				cet_from = utc_from.astimezone(cet_timezone)

				# Use 'imbalanceNettingImport' and 'imbalanceNettingExport' fields if they exist
				netting_import = float(item.get("imbalanceNettingImport", 0) or 0)
				netting_export = float(item.get("imbalanceNettingExport", 0) or 0)

				rows.append([cet_from, netting_import, netting_export])
			except Exception as item_error:
				print(f"Error processing item for IGCC data: {item_error}") # Catch errors for individual items

		if not rows:
			print("⚠️ No valid IGCC data rows processed.")
			return pd.DataFrame(columns=["Timestamp", "IGCC Import (MW)", "IGCC Export (MW)"])

		df = pd.DataFrame(rows, columns=["Timestamp", "IGCC Import (MW)", "IGCC Export (MW)"])
		# Ensure Timestamp column is datetime and remove timezone for consistency if needed later
		df["Timestamp"] = pd.to_datetime(df["Timestamp"]).dt.tz_localize(None)+timedelta(minutes=15)

		return df

	except requests.exceptions.RequestException as req_error:
		print(f"❌ Request failed for IGCC data: {req_error}")
		return pd.DataFrame(columns=["Timestamp", "IGCC Import (MW)", "IGCC Export (MW)"])
	except Exception as json_error: # Catch JSON parsing errors or other issues
		print(f"❌ Error processing IGCC data response: {json_error}")
		return pd.DataFrame(columns=["Timestamp", "IGCC Import (MW)", "IGCC Export (MW)"])

#Unintended Deviation================================================================================
def fetch_unintended_deviation_data():
	"""Fetch estimated unintended deviations for import and export, converting timestamps to CET."""

	# Define timezone for CET (handles DST automatically)
	cet_timezone = pytz.timezone('Europe/Bucharest')

	# Get current date in **CET** and set midnight as start of the day
	cet_now = datetime.now(cet_timezone)
	cet_midnight = cet_now.replace(hour=0, minute=0, second=0, microsecond=0)

	# Convert midnight CET to UTC (since API operates in UTC)
	utc_midnight = cet_midnight.astimezone(pytz.utc)

	# Set API time range: Fetch from **CET midnight (converted to UTC) until now**
	from_time = utc_midnight.strftime("%Y-%m-%dT%H:%M:%S.000Z")
	to_time = (utc_midnight + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

	# API Request
	url = f"https://newmarkets.transelectrica.ro/usy-durom-publicreportg01/00121002500000000000000000000100/publicReport/estimatedPowerSystemImbalance?timeInterval.from={from_time}&timeInterval.to={to_time}&pageInfo.pageSize=3000"

	response = requests.get(url)

	if response.status_code != 200:
		print(f"⚠️ Failed to fetch data. Status code: {response.status_code}")
		return pd.DataFrame()

	# Parse JSON response
	try:
		data = response.json()
		items = data.get("itemList", [])

		print(f"✅ Successfully fetched {len(items)} records.")

		if len(items) == 0:
			print("⚠️ No data found in itemList.")
			return pd.DataFrame()

		# Process and convert timestamps
		rows = []
		for item in items:
			try:
				# Convert timestamps from UTC to CET
				utc_from = datetime.fromisoformat(item['timeInterval']['from'].replace('Z', '+00:00'))
				utc_to = datetime.fromisoformat(item['timeInterval']['to'].replace('Z', '+00:00'))

				cet_from = utc_from.astimezone(cet_timezone)
				cet_to = utc_to.astimezone(cet_timezone)

				# Store in formatted string
				time_period = f"{cet_from.strftime('%Y-%m-%d %H:%M:%S')} - {cet_to.strftime('%Y-%m-%d %H:%M:%S')}"

				# ✅ Correct field names
				unintended_import = float(item.get("estimatedUnintendedDeviationINArea", 0) or 0)
				unintended_export = float(item.get("estimatedUnintendedDeviationOUTArea", 0) or 0)

				# Debugging - Print all added records
				print(f"ADDING: {time_period} | IN: {unintended_import}, OUT: {unintended_export}")

				# Store processed row
				rows.append([cet_from, unintended_import, unintended_export])

			except Exception as e:
				print(f"❌ Error processing record: {e}")

		# Convert to DataFrame
		df_unintended_deviation = pd.DataFrame(rows, columns=['Timestamp', 'Unintended_Import (MW)', 'Unintended_Export (MW)'])

		# Ensure Timestamp is properly formatted in CET
		df_unintended_deviation['Timestamp'] = pd.to_datetime(df_unintended_deviation['Timestamp']).dt.tz_localize(None)

		return df_unintended_deviation

	except Exception as e:
		print(f"❌ JSON Parsing Error: {e}")
		return pd.DataFrame()

# Function to build the balancing market context in EET==========================================
def build_balancing_market_context_eet():
    # Fetching the core inputs
    df_imbalance_volumes = fetch_intraday_imbalance_volumes()
    df_imbalance_prices = fetch_intraday_imbalance_prices()
    df_imbalance = create_combined_imbalance_dataframe(df_imbalance_prices, df_imbalance_volumes)
    df_igcc = fetch_igcc_netting_flows()
    df_unintended_deviation = fetch_unintended_deviation_data()
    activation_df = fetch_balancing_energy_data()
    price_df = fetch_marginal_prices()
    # Merge and display
    merged_df = pd.merge(activation_df, price_df, on="Time Period (EET)", how="left")

    # Step 4: Extract Timestamp from start of time period
    merged_df["Timestamp"] = merged_df["Time Period (EET)"].str.split(" - ").str[1]
    merged_df["Timestamp"] = pd.to_datetime(merged_df["Timestamp"], errors="coerce")
    merged_df["Timestamp"] = merged_df["Timestamp"].dt.tz_localize("Europe/Bucharest", ambiguous="NaT", nonexistent="shift_forward").dt.tz_localize(None)

    # Step 5: Drop the old "Time Period (EET)" column
    merged_df.drop(columns=["Time Period (EET)"], inplace=True)

    # Step 6: Merge imbalance + activation+prices on Timestamp
    df_final = pd.merge(df_imbalance, merged_df, on="Timestamp", how="outer")

    # --- Step 4: Fetch and merge IGCC flows ---
    df_igcc["Timestamp"] = pd.to_datetime(df_igcc["Timestamp"]).dt.tz_localize("Europe/Bucharest", ambiguous="NaT", nonexistent="shift_forward").dt.tz_localize(None)

    df_final = pd.merge(df_final, df_igcc, on="Timestamp", how="left")
    df_final = pd.merge(df_final, df_unintended_deviation, on="Timestamp", how="left")
    # --- Step 5: Final cleanup ---
    df_final = df_final.sort_values("Timestamp").reset_index(drop=True)

    # Fill missing values for numeric columns
    energy_cols = [
        "aFRR Up (MWh)", "aFRR Down (MWh)",
        "mFRR Up (MWh)", "mFRR Down (MWh)",
        "aFRR Up Price (RON/MWh)", "aFRR Down Price (RON/MWh)",
        "mFRR Up Price (RON/MWh)", "mFRR Down Price (RON/MWh)",
        "Excedent Price", "Deficit Price", "Imbalance Volume",
        "IGCC Import (MW)", "IGCC Export (MW)"
    ]
    for col in energy_cols:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna(0)

    return df_final
    
# Function to detect and handle alarms================================================
def check_balancing_alarms(df):
    warning_alarms = []
    critical_alarms = []
    all_alarms = []

    # Get current time for reference
    current_time = datetime.now()

    # Check for no data update within 20 minutes (Critical Alarm)
    if not df.empty:
        latest_timestamp = datetime.strptime(df.iloc[-1]["Time Period (EET)"].split(" - ")[1], "%Y-%m-%d %H:%M:%S")
        time_diff = current_time - latest_timestamp

        if time_diff > timedelta(minutes=20):
            message = f"🚨 Critical: No new data received for {time_diff.seconds // 60} minutes (last update at {latest_timestamp})."
            alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
            alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
            all_alarms.append((alarm_id, message, "Critical"))

            if message not in critical_alarms:
                critical_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Critical"))

    else:
        message = "🚨 Critical: No data available from the server."
        if message not in critical_alarms:
            critical_alarms.append(message)
            alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
            alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
            all_alarms.append((alarm_id, message, "Critical"))

    for i in range(1, len(df)):

        # Current and previous interval values
        current_mFRR_up = df.iloc[i]["mFRR Up (MWh)"] or 0
        current_mFRR_down = df.iloc[i]["mFRR Down (MWh)"] or 0
        current_aFRR_up = df.iloc[i]["aFRR Up (MWh)"] or 0
        current_aFRR_down = df.iloc[i]["aFRR Down (MWh)"] or 0

        previous_mFRR_up = df.iloc[i-1]["mFRR Up (MWh)"] or 0
        previous_mFRR_down = df.iloc[i-1]["mFRR Down (MWh)"] or 0
        previous_aFRR_up = df.iloc[i-1]["aFRR Up (MWh)"] or 0
        previous_aFRR_down = df.iloc[i-1]["aFRR Down (MWh)"] or 0

        # Current and previous interval values for total activation
        current_total_up = current_aFRR_up + current_mFRR_up
        current_total_down = current_aFRR_down + current_mFRR_down
        previous_total_up = previous_aFRR_up + previous_mFRR_up
        previous_total_down = previous_aFRR_down + previous_mFRR_down

        # Debugging print statements
        print(f"Row {df.index[i]}: prev_up={previous_total_up}, prev_down={previous_total_down}, curr_up={current_total_up}, curr_down={current_total_down}")

        # ================= Critical Alarm: System Change of Direction =================
        if previous_total_up > previous_total_down and current_total_down > current_total_up:
            message = (f"🚨 Critical: System switched from upward total activation to downward "
                       f"total activation at {df.index[i]}")
            if message not in critical_alarms:
                print(f"Triggering Critical Alarm: {message}")
                critical_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Critical"))

        if previous_total_down > previous_total_up and current_total_up > current_total_down:
            message = (f"🚨 Critical: System switched from downward total activation to upward "
                       f"total activation at {df.index[i]}")
            if message not in critical_alarms:
                print(f"Triggering Critical Alarm: {message}")
                critical_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Critical"))

        # ================= Warning Alarms ============================================
        if current_mFRR_up < previous_mFRR_up and current_aFRR_down > previous_aFRR_down:
            message = f"⚠️ Warning: mFRR Up decreasing and aFRR Down increasing at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Warning"))

        if current_mFRR_down < previous_mFRR_down and current_aFRR_up > previous_aFRR_up:
            message = f"⚠️ Warning: mFRR Down decreasing and aFRR Up increasing at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Warning"))

        # Rate of change in mFRR Up and Down
        rate_of_change_up = current_mFRR_up - previous_mFRR_up
        rate_of_change_down = current_mFRR_down - previous_mFRR_down

        # Check for sudden increase or drop in mFRR Up
        if abs(rate_of_change_up) >= RATE_OF_CHANGE_THRESHOLD:
            if rate_of_change_up > 0:
                message = f"⚠️ Warning: Sudden increase in mFRR Up by {rate_of_change_up} MWh at {df.index[i]}"
            else:
                message = f"⚠️ Warning: Sudden drop in mFRR Up by {abs(rate_of_change_up)} MWh at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Warning"))

        # Check for sudden increase or drop in mFRR Down
        if abs(rate_of_change_down) >= RATE_OF_CHANGE_THRESHOLD:
            if rate_of_change_down > 0:
                message = f"⚠️ Warning: Sudden increase in mFRR Down by {rate_of_change_down} MWh at {df.index[i]}"
            else:
                message = f"⚠️ Warning: Sudden drop in mFRR Down by {abs(rate_of_change_down)} MWh at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Warning"))

        # ================= Critical Alarms ====================================
        if previous_mFRR_up > 0 and current_mFRR_down > 0:
            message = f"🚨 Critical: System switched from deficit to surplus at {df.index[i]}"
            if message not in critical_alarms:
                critical_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Critical"))

        if previous_mFRR_down > 0 and current_mFRR_up > 0:
            message = f"🚨 Critical: System switched from surplus to deficit at {df.index[i]}"
            if message not in critical_alarms:
                critical_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Critical"))

        # Opposite aFRR activation (aFRR spike in opposite direction)
        if previous_aFRR_up > previous_aFRR_down and previous_aFRR_up > THRESHOLD_AFRR_UP:
            if current_aFRR_down > previous_aFRR_down and current_aFRR_down > THRESHOLD_AFRR_DOWN:
                message = f"🚨 Critical: Sudden spike in aFRR Down at {df.index[i]}"
                if message not in critical_alarms:
                    critical_alarms.append(message)
                    alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                    alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                    all_alarms.append((alarm_id, message, "Critical"))

        if previous_aFRR_down > previous_aFRR_up and previous_aFRR_down > THRESHOLD_AFRR_DOWN:
            if current_aFRR_up > previous_aFRR_up and current_aFRR_up > THRESHOLD_AFRR_UP:
                message = f"🚨 Critical: Sudden spike in aFRR Up at {df.index[i]}"
                if message not in critical_alarms:
                    critical_alarms.append(message)
                    alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                    alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                    all_alarms.append((alarm_id, message, "Critical"))

          # ================= aFRR-Only Imbalance Detection ===========================

        # aFRR-only dominance and system direction shift
        if previous_aFRR_up > previous_aFRR_down and current_aFRR_down > current_aFRR_up:
            message = f"⚠️ Warning: aFRR switched from Up to Down dominance at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Warning"))

        if previous_aFRR_down > previous_aFRR_up and current_aFRR_up > current_aFRR_down:
            message = f"⚠️ Warning: aFRR switched from Down to Up dominance at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Warning"))

        # mFRR deactivation when it is active but there is no update when the difference between the current time and the beggining of the next quarter is less than 9 minutes
        # Ensure we have enough data before checking
        if len(df) >= 2:
            # Get the latest two timestamps in EET
            latest_timestamp = datetime.strptime(df.iloc[-1]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
            latest_timestamp = eet_timezone.localize(latest_timestamp)

            previous_timestamp = datetime.strptime(df.iloc[-2]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
            previous_timestamp = eet_timezone.localize(previous_timestamp)

            # Compute the next expected interval start time
            expected_next_interval = latest_timestamp + timedelta(minutes=15)

            # Get the current time in EET
            current_time = datetime.now(eet_timezone)

            # Calculate time difference between expected interval and current time
            missing_time = (expected_next_interval - current_time).total_seconds() / 60

            print(f"latest_timestamp (EET): {latest_timestamp} | tzinfo: {latest_timestamp.tzinfo}")
            print(f"expected_next_interval (EET): {expected_next_interval} | tzinfo: {expected_next_interval.tzinfo}")
            print(f"current_time (EET): {current_time} | tzinfo: {current_time.tzinfo}")
            print(f"Missing time before next quarter: {missing_time:.2f} minutes")

            # Check if there was mFRR activation in the last **two intervals** but no update for the next one
            last_mFRR_active = df.iloc[-1]["mFRR Up (MWh)"] > 0 or df.iloc[-1]["mFRR Down (MWh)"] > 0
            prev_mFRR_active = df.iloc[-2]["mFRR Up (MWh)"] > 0 or df.iloc[-2]["mFRR Down (MWh)"] > 0

            # If missing_time <= 9 minutes OR the next interval is already ongoing, trigger alarm
            if (last_mFRR_active or prev_mFRR_active) and (missing_time <= 9 or current_time >= expected_next_interval):
                message = (f"🚨 Critical: No new mFRR update detected for the next interval starting at {expected_next_interval}. "
                           f"The next interval may rely solely on aFRR.")

                # Generate an alarm ID using the expected interval timestamp
                alarm_id = expected_next_interval.timestamp()

                # Prevent duplicates
                if message not in critical_alarms:
                    critical_alarms.append(message)
                    all_alarms.append((alarm_id, message, "Critical"))
                    print(f"Triggering Critical Alarm: {message}")

        # Large spikes in aFRR
        aFRR_spike_up = abs(current_aFRR_up - previous_aFRR_up)
        aFRR_spike_down = abs(current_aFRR_down - previous_aFRR_down)

        if aFRR_spike_up >= AFRR_SPIKE_THRESHOLD:
            message = f"🚨 Critical: Sudden large spike in aFRR Up by {aFRR_spike_up} MWh at {df.index[i]}"
            if message not in critical_alarms:
                critical_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Critical"))

        if aFRR_spike_down >= AFRR_SPIKE_THRESHOLD:
            message = f"🚨 Critical: Sudden large spike in aFRR Down by {aFRR_spike_down} MWh at {df.index[i]}"
            if message not in critical_alarms:
                critical_alarms.append(message)
                alarm_id = datetime.strptime(df.iloc[i]["Time Period (EET)"].split(" - ")[0], "%Y-%m-%d %H:%M:%S")
                alarm_id = eet_timezone.localize(alarm_id).timestamp()  # Convert to timestamp
                all_alarms.append((alarm_id, message, "Critical"))

        # Check for new critical alarms and make a call if any
        new_critical_alarms = [alarm for alarm in critical_alarms if alarm not in st.session_state["calls_made_critical"]]
        if new_critical_alarms:
            for alarm in new_critical_alarms:
                if is_valid_phone_number(USER_PHONE_NUMBER):
                    # Ensure alarm ID is numeric if possible, otherwise use timestamp as fallback
                    if isinstance(alarm[0], (int, float)):  
                        alarm_id = int(alarm[0])  # Standard case, keep as integer
                    else:
                        # Generate an ID based on the expected next interval timestamp (for mFRR deactivation alarms)
                        alarm_id = int(datetime.now().timestamp())  # Use Unix timestamp as unique ID
                    
                    print(f"🔔 Calling for Critical Alarm: {alarm}, ID: {alarm_id}")
                    make_call("Critical", alarm, alarm_id)
                    st.session_state["calls_made_critical"].append(alarm)  # Store it immediately

        # Check for new warning alarms and make a call if not during night hours
        if not is_night_time():
            new_warning_alarms = [alarm for alarm in warning_alarms if alarm not in st.session_state["calls_made_warning"]]
            if new_warning_alarms:
                for alarm in new_warning_alarms:
                    if is_valid_phone_number(USER_PHONE_NUMBER):
                        # Ensure alarm ID consistency
                        if isinstance(alarm[0], (int, float)):  
                            alarm_id = int(alarm[0])  # Standard case, keep as integer
                        else:
                            alarm_id = int(datetime.now().timestamp())  # Use Unix timestamp as unique ID
                        
                        print(f"🔔 Calling for Warning Alarm: {alarm}, ID: {alarm_id}")
                        make_call("Warning", alarm, alarm_id)
                        st.session_state["calls_made_warning"].append(alarm)


    # Append only new alarms to the stored session state
    for alarm in all_alarms:
        if alarm not in st.session_state["all_alarms"]:
            st.session_state["all_alarms"].append(alarm)

    # Return stored alarms to display in UI
    return st.session_state["all_alarms"]

# Adding the expert advisor
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def encode_image_to_base64(img_file):
    """
    Converts an uploaded image (from st.file_uploader) to base64 format
    for use in GPT-4 Vision's image_url API.
    """
    return b64encode(img_file.read()).decode("utf-8")

# ---------------------------------------------------------------------------
# Helper -- turn the EET-aligned DataFrame into compact JSON rows
# ---------------------------------------------------------------------------
def df_to_json_rows(df, keep_cols=None, round_floats=3):
    """
    Convert DataFrame → JSON (list-of-dicts).
    • Floats are rounded
    • NaNs → 0
    • pd.Timestamp / datetime → ISO string
    """
    import json, numpy as np, pandas as pd, datetime as dt

    if keep_cols is not None:
        df = df[keep_cols]

    def fix(v):
        # ----- handle timestamps -------
        if isinstance(v, (pd.Timestamp, dt.datetime)):
            # keep timezone info if present
            return v.isoformat()
        # ----- floats ----------
        if isinstance(v, (float, np.floating)):
            return round(float(v), round_floats)
        # ----- NaN -------------
        if pd.isna(v):
            return 0
        return v

    rows = [{k: fix(v) for k, v in row.items()} for row in df.to_dict("records")]
    return json.dumps(rows, ensure_ascii=False)
def clean_model_response(raw: str) -> dict:
    """
    Clean and parse the JSON response from the model.
    Removes markdown code block wrappers and returns parsed JSON.
    """
    cleaned = re.sub(r"^```json|```$", "", raw.strip(), flags=re.MULTILINE).strip()
    return json.loads(cleaned)

def call_expert(latest_context_df, notes=""):
    """
    Call the reasoning LLM to forecast Balancing-Market state.
    • Primary model: gpt-4-o3
    • Fallback model: o3-mini
    Returns dict with keys: result (parsed JSON) & model_used
    On total failure returns dict with 'error'.
    """
    import json
    from openai import OpenAI

    client = OpenAI()
    o3_err = None
    fallback_err = None

    cols = [
        "Timestamp",
        "Imbalance Volume",
        "IGCC Export (MW)", "IGCC Import (MW)",
        "aFRR Up (MWh)", "aFRR Down (MWh)",
        "mFRR Up (MWh)", "mFRR Down (MWh)",
        "aFRR Up Price (RON/MWh)", "aFRR Down Price (RON/MWh)",
        "mFRR Up Price (RON/MWh)", "mFRR Down Price (RON/MWh)"
    ]

    df = latest_context_df[cols].copy()
    df["Timestamp"] = df["Timestamp"].astype(str)

    # Use sanitized key format for JSON prompt
    df.columns = [col.strip().replace(" ", "_").replace("(", "").replace(")", "") for col in df.columns]
    json_rows = json.dumps(df.to_dict(orient="records"), indent=2, ensure_ascii=False)
    # --- 2) Beast-level Expert Prompt ---
    system_instruction = """
ROLE  
You are the Lead Balancing-Market Trader for a 500 MW renewable portfolio in Romania.  
You routinely outperform OPCOM day-ahead by >15 €/MWh via superior imbalance forecasting  
and proactive, risk-adjusted strategy execution.

=== DATA PROVIDED (JSON array 'DATA') ===  
• Timestamp (EET, 15-min rows)  
• Imbalance Volume MWh (+ = surplus, – = deficit)  
• Deficit & Excedent Prices (RON/MWh)  
• aFRR Up/Down & mFRR Up/Down activations (MWh)  
• Marginal prices for each service (RON/MWh)  
• IGCC Import / Export (MW)

=== ADVANCED INTERPRETATION RULES ===  

1. Supply–Demand Lens  
• Surplus = +Imbalance + IGCC Export + mFRR Down + Neg Deficit Price  
• Deficit = –Imbalance + IGCC Import + mFRR Up + High Deficit Price

2. Price Skew & Elasticity  
• Elasticity ≈ 0.4 RON/MWh mFRR  
• mFRR Down > 50 + Deficit Price < 0 → 80% chance price < –200 next interval

3. Momentum Detection (Reinforcement Logic)  
• 3+ Surplus = 85% chance of surplus continuing, unless mFRR Up or IGCC reverses  
• Detect micro-shifts: spikes in aFRR/mFRR with no volume change imply latent signals  
• IGCC flow reversal ±20 MW is often the first sign of trend change

4. Risk–Reward Zones  
• mFRR Down > 120 MWh OR Deficit-Price < –300 → "Crash-risk"  
• IGCC Import > 150 OR Deficit-Price > 700 → "Spike-risk"

5. Action Engine (Tactical Playbook)  
• Crash-risk & Surplus → Curtail 50–100 MW, bid BM Down at –10  
• Spike-risk & Deficit → Start Gen, pre-bid mFRR Up at 690  
• Trend = Stable? → Watch for arbitrage vs IDM spread

6. aFRR Visibility  
• aFRR is delayed ⇒ treat 0 as "not yet published"  
• mFRR = real-time ⇒ treat mFRR Up/Down > 0 as predictive signal  
• If Imbalance = 0 AND aFRR = 0 ⇒ Interval is unpublished — skip reasoning

7. Forecasting Forward  
• Predict trend over next 2 intervals  
• If Deficit persists + mFRR Up = 0 ⇒ Forecast upcoming spike  
• If mFRR Down rising + Imbalance + ⇒ Trend toward price drop

8. Anticipation Layer  
• Assign confidence % to continuation vs reversal  
• Suggest alternate paths (ex: “If IGCC reverses, risk of spike rises 40%”)  
• Include reinforcement-style advice: what to monitor next, how to hedge

=== OUTPUT (JSON ONLY) ===

{
  "state": "Surplus" or "Deficit",
  "confidence": float (0.0–1.0),
  "expected_trend_next_interval": "Upward" or "Stable" or "Reversal",
  "prob_neg_price": float,
  "prob_pos_price": float,
  "premises": ["Observation 1", "Observation 2"],
  "strategy": "Tactical action (≤20 words)",
  "rationale": "Why this action makes sense (≤40 words)",
  "risk_note": "Key risk (≤15 words)",
  "forward_scenarios": [
    "If mFRR Up increases by 50 MWh → trend reversal possible",
    "If IGCC Export drops > 30 MW → price risk to upside"
  ],
  "next_monitoring_focus": "mFRR Up spikes, IGCC reversal, aFRR confirmation"
}

Return valid JSON only.
"""
    user_prompt = f"""
        You are a Balancing Market expert.

        Below is today's real-time 15-min interval context, labeled as DATA.
        Use this to forecast the next system state, anticipate near-future trends, and recommend action.

        DATA:
        {json_rows}

        Trader Notes: {notes or "None"}

        Respond in the strict JSON format defined by the system. Output expert-level insight, not generic advice.
        """

    st.write(user_prompt)
    # === Try GPT-4o ===
    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": user_prompt}
            ],
            max_completion_tokens=1500
        )
        raw_content = resp.choices[0].message.content
        print(100*"=")
        print("✅ o3 Response:", repr(raw_content))
        print(100*"=")
        return {
            "result": clean_model_response(raw_content),
            "model_used": "gpt-4o"
        }

    except Exception as e:
        o3_err = e
        print("❌ gpt-4-o3 failed:", e)

        # === Try o3-mini ===
        try:
            resp = client.chat.completions.create(
                model="o3-mini",
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": user_prompt}
                ],
                max_completion_tokens=800
            )
            raw_content = resp.choices[0].message.content
            print("🔁 o3-mini response:", repr(raw_content))

            return {
                "result": clean_model_response(raw_content),
                "model_used": "o3-mini"
            }

        except json.JSONDecodeError as je:
            print("⚠️ JSON parse error:", je)
            return {
                "error": "Both gpt-4-o3 and o3-mini failed (JSON parsing).",
                "o3_error": str(o3_err) if o3_err else "n/a",
                "fallback_error": f"Invalid JSON: {je}"
            }

        except Exception as fallback_err:
            print("❌ o3-mini failed:", fallback_err)
            return {
                "error": "Both gpt-4-o3 and o3-mini failed.",
                "o3_error": str(o3_err) if o3_err else "n/a",
                "fallback_error": str(fallback_err)
            }

# Creating the context for the expert advisor===============================================
# Display in Streamlit for debugging
st.subheader("📊 Full Balancing Market Context (EET-aligned)")
imbalance_volumes_df = fetch_intraday_imbalance_volumes()
st.write(imbalance_volumes_df)
st.write(fetch_intraday_imbalance_prices())
st.write(fetch_igcc_netting_flows())
df_context = build_balancing_market_context_eet()
st.dataframe(df_context)
st.dataframe(fetch_unintended_deviation_data())

def test_o3_mini_connectivity():
    from openai import OpenAI
    client = OpenAI()

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Who are you?"}
            ],
            max_completion_tokens=200,
        )

        print("📬 Raw OpenAI Response:"+100*"=", response)
        content = response.choices[0].message.content
        print("🧠 Model Output:", repr(content))  # Use repr to show trailing whitespace
        return content

    except Exception as e:
        print("❌ Error:", e)
        return None

if st.button("Test OpenAI"):
    st.write(test_o3_mini_connectivity())

# Layout for the app with columns
col1, col2 = st.columns([5, 1])  # Table takes 2/3 width, alarms take 1/3 width

with col1:
    st.subheader("Balancing Market Data")
    
    # Show current EET timestamp
    current_time_eet = datetime.now().astimezone(eet_timezone).strftime("%Y-%m-%d %H:%M:%S")
    st.info(f"Last updated: **{current_time_eet}**")

    # Fetch both datasets
    activation_df = fetch_balancing_energy_data()
    price_df = fetch_marginal_prices()

    # Merge and display
    if not activation_df.empty and not price_df.empty:
        merged_df = pd.merge(activation_df, price_df, on="Time Period (EET)", how="left")

        # Ensure sorted display by interval start
        merged_df["Start Time"] = pd.to_datetime(merged_df["Time Period (EET)"].str.split(" - ").str[0])
        merged_df = merged_df.sort_values(by="Start Time").drop(columns=["Start Time"])

        st.dataframe(merged_df, use_container_width=True)

    else:
        if activation_df.empty:
            st.warning("⚠️ Activation energy data is not available.")
        if price_df.empty:
            st.warning("⚠️ Marginal price data is not available.")
    st.markdown("### 🧠 Expert Advisor")

    # Creating the form for the expert advisor
    with st.form("expert_advisor_form"):
        st.markdown("#### 📎 Add Notes")

        trader_notes = st.text_area(
            "Your Observations or Strategy Notes",
            placeholder="E.g., Wind ramping +200 MW, IGCC is strongly negative..."
        )

        submitted = st.form_submit_button("🔍 Call Expert Advisor")

        if submitted:
            # Build the full context
            full_context_df = build_balancing_market_context_eet()

            # Defensive filtering + sort
            full_context_df = full_context_df.dropna(subset=["Timestamp"]).sort_values("Timestamp")

            # Get only the last 8 intervals (past)
            now = pd.Timestamp.now(tz="Europe/Bucharest")
            full_context_df["Timestamp"] = pd.to_datetime(full_context_df["Timestamp"]).dt.tz_localize("Europe/Bucharest")
            latest_context_df = full_context_df[full_context_df["Timestamp"] < now].tail(12)
            st.dataframe(latest_context_df)
            # Call the expert model
            expert_result = call_expert(latest_context_df, notes=trader_notes)

            if "error" in expert_result:
                st.error(f"❌ Error: {expert_result['error']}")
                if expert_result.get("fallback_error"):
                    st.code(f"[Fallback Error] {expert_result['fallback_error']}", language="text")
                elif expert_result.get("o3_error"):
                    st.code(f"[o3 Error] {expert_result['o3_error']}", language="text")
            else:
                result = expert_result["result"]
                model_used = expert_result["model_used"]

                # ——— 1. Unpublished Interval Warning ———
                if ((latest_context_df["Imbalance Volume"] == 0) &
                    (latest_context_df["aFRR Up (MWh)"] == 0) &
                    (latest_context_df["aFRR Down (MWh)"] == 0)).any():
                    st.warning("⚠️ One or more intervals appear to be unpublished. Don't interpret them as balanced.")

                # ——— 2. Persistent Trend Check (last 12 intervals from full_context_df) ———
                last_vols = full_context_df["Imbalance Volume"].dropna().tail(12)
                if (last_vols > 0).sum() >= 3:
                    st.info("📈 3+ Surplus intervals detected. Trend may persist unless mFRR Up increases.")
                elif (last_vols < 0).sum() >= 3:
                    st.info("📉 3+ Deficit intervals detected. Trend may persist unless IGCC Export or mFRR Down increases.")

                # ——— 3. Display AI Output ———
                st.success(f"✅ System State: {result['state']} ({int(result['confidence'] * 100)}% confidence)")
                st.markdown(f"**🤖 Powered by:** `{model_used}`")
                st.markdown(f"**📊 Trend Next Interval:** {result['expected_trend_next_interval']}")

                st.markdown("**📌 Premises:**")
                for p in result["premises"]:
                    st.markdown(f"- {p}")

                st.markdown(f"**🛠 Strategy:** {result['strategy']}")
                st.markdown(f"**🧠 Rationale:** {result['rationale']}")

                # ——— 4. Risk Note with Color Badge ———
                risk_text = result["risk_note"].strip()
                risk_lower = risk_text.lower()
                if "crash" in risk_lower or "neg" in risk_lower or "spike" in risk_lower:
                    risk_color = "red"
                elif "watch" in risk_lower or "likely" in risk_lower:
                    risk_color = "orange"
                else:
                    risk_color = "green"

                st.markdown(
                    f"**⚠️ Risk Note:** <span style='color:{risk_color}; font-weight:bold'>{risk_text}</span>",
                    unsafe_allow_html=True
                )

with col2:
    st.subheader("Alarms Triggered")
    all_alarms = check_balancing_alarms(merged_df)

    # Sort alarms chronologically before displaying
    st.session_state["all_alarms"].sort(key=lambda x: x[0], reverse=True)

    if all_alarms:
        for timestamp, message, alarm_type in all_alarms:
            if alarm_type == "Critical":
                st.error(message)
            else:
                st.warning(message)
    else:
        st.success("✅ No alarms triggered.")

async def refresh_app(interval_seconds):
    await asyncio.sleep(interval_seconds)
    st.rerun()

# Trigger the auto-refresh
# asyncio.run(refresh_app(60))  # Refresh every 60 seconds

