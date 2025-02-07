import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import pytz
import threading
from playsound import playsound
import time
import base64
from twilio.rest import Client
from dotenv import load_dotenv
import os

# Page configuration for wide layout
st.set_page_config(layout="wide")

# Initialize session state for alarm tracking
if "last_alarm_count" not in st.session_state:
    st.session_state["last_alarm_count"] = 0
if "stored_alarms" not in st.session_state:
    st.session_state["stored_alarms"] = []

# Thresholds for triggering alarms
THRESHOLD_AFRR_UP = 20
THRESHOLD_AFRR_DOWN = 15
THRESHOLD_MFRR_UP = 30
THRESHOLD_MFRR_DOWN = 25
RATE_OF_CHANGE_THRESHOLD = 20
AFRR_SPIKE_THRESHOLD = 25

# Load environment variables from .env file
load_dotenv()

# Twilio account configuration
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")
USER_PHONE_NUMBER = os.getenv("USER_PHONE_NUMBER")

# Initialize Twilio client
client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# Function to make a phone call
def make_test_call():
    try:
        print("Initiating test call...")
        call = client.calls.create(
            twiml='<Response><Say>Hello! This is a test call from your energy monitoring app.</Say></Response>',
            to=USER_PHONE_NUMBER,
            from_=TWILIO_PHONE_NUMBER
        )
        print(f"Call initiated successfully! Call SID: {call.sid}")
    except Exception as e:
        print(f"Error making the call: {e}")

# Function to fetch and convert data to EET
def fetch_balancing_energy_data():
    # Get current date and set midnight as the start of the day
    today = datetime.now()
    midnight_today = today.replace(hour=0, minute=0, second=0, microsecond=0)

    # Format dates in ISO 8601 format for the API
    from_time = midnight_today.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to_time = today.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Dynamic API request URL for the current day
    url = f"https://newmarkets.transelectrica.ro/usy-durom-publicreportg01/00121002500000000000000000000100/publicReport/activatedBalancingEnergyOverview?timeInterval.from={from_time}&timeInterval.to={to_time}&pageInfo.pageSize=3000"

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        items = data["itemList"]

        # Process rows and convert timestamps
        rows = []
        for item in items:
            # Convert timestamps from UTC to EET
            utc_from = datetime.fromisoformat(item['timeInterval']['from'].replace('Z', '+00:00'))
            utc_to = datetime.fromisoformat(item['timeInterval']['to'].replace('Z', '+00:00'))

            # Convert to EET
            eet_timezone = pytz.timezone('Europe/Bucharest')
            eet_from = utc_from.astimezone(eet_timezone).strftime("%Y-%m-%d %H:%M:%S")
            eet_to = utc_to.astimezone(eet_timezone).strftime("%Y-%m-%d %H:%M:%S")

            # Extract energy values
            afrr_up = item.get("aFRR_Up", 0)
            afrr_down = item.get("aFRR_Down", 0)
            mfrr_up = item.get("mFRR_Up", 0)
            mfrr_down = item.get("mFRR_Down", 0)

            # Append the processed row
            rows.append([f"{eet_from} - {eet_to}", afrr_up, afrr_down, mfrr_up, mfrr_down])

        # Create DataFrame
        return pd.DataFrame(rows, columns=["Time Period (EET)", "aFRR Up (MWh)", "aFRR Down (MWh)", "mFRR Up (MWh)", "mFRR Down (MWh)"])
    else:
        st.error(f"Failed to fetch data. Status code: {response.status_code}")
        return pd.DataFrame()

# Function to detect and handle alarms
def check_balancing_alarms(df):
    alarms = []
    alarm_triggered = False  # Track if any alarm is triggered

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

        # ================= Warning Alarms ================================
        if current_mFRR_up < previous_mFRR_up and current_aFRR_down > previous_aFRR_down:
            message = f"⚠️ Warning: mFRR Up decreasing and aFRR Down increasing at {df.index[i]}"
            alarms.append(message)

        if current_mFRR_down < previous_mFRR_down and current_aFRR_up > previous_aFRR_up:
            message = f"⚠️ Warning: mFRR Down decreasing and aFRR Up increasing at {df.index[i]}"
            alarms.append(message)

        # Large drop in mFRR (rate of change)
        rate_of_change_up = abs(current_mFRR_up - previous_mFRR_up)
        rate_of_change_down = abs(current_mFRR_down - previous_mFRR_down)

        if rate_of_change_up >= RATE_OF_CHANGE_THRESHOLD:
            message = f"⚠️ Warning: Sudden drop in mFRR Up by {rate_of_change_up} MWh at {df.index[i]}"
            alarms.append(message)

        if rate_of_change_down >= RATE_OF_CHANGE_THRESHOLD:
            message = f"⚠️ Warning: Sudden drop in mFRR Down by {rate_of_change_down} MWh at {df.index[i]}"
            alarms.append(message)

        # ================= Critical Alarms ====================================
        if previous_mFRR_up > 0 and current_mFRR_down > 0:
            message = f"🚨 Critical: System switched from deficit to surplus at {df.index[i]}"
            alarms.append(message)

        if previous_mFRR_down > 0 and current_mFRR_up > 0:
            message = f"🚨 Critical: System switched from surplus to deficit at {df.index[i]}"
            alarms.append(message)

        # Opposite aFRR activation (aFRR spike in opposite direction)
        if previous_aFRR_up > previous_aFRR_down and previous_aFRR_up > THRESHOLD_AFRR_UP:
            if current_aFRR_down > previous_aFRR_down and current_aFRR_down > THRESHOLD_AFRR_DOWN:
                message = f"🚨 Critical: Sudden spike in aFRR Down at {df.index[i]}"
                alarms.append(message)

        if previous_aFRR_down > previous_aFRR_up and previous_aFRR_down > THRESHOLD_AFRR_DOWN:
            if current_aFRR_up > previous_aFRR_up and current_aFRR_up > THRESHOLD_AFRR_UP:
                message = f"🚨 Critical: Sudden spike in aFRR Up at {df.index[i]}"
                alarms.append(message)

          # ================= aFRR-Only Imbalance Detection ===========================

        # aFRR-only dominance and system direction shift
        if previous_aFRR_up > previous_aFRR_down and current_aFRR_down > current_aFRR_up:
            message = f"⚠️ Warning: aFRR switched from Up to Down dominance at {df.index[i]}"
            alarms.append(message)

        if previous_aFRR_down > previous_aFRR_up and current_aFRR_up > current_aFRR_down:
            message = f"⚠️ Warning: aFRR switched from Down to Up dominance at {df.index[i]}"
            alarms.append(message)

        # Large spikes in aFRR
        aFRR_spike_up = abs(current_aFRR_up - previous_aFRR_up)
        aFRR_spike_down = abs(current_aFRR_down - previous_aFRR_down)

        if aFRR_spike_up >= AFRR_SPIKE_THRESHOLD:
            message = f"🚨 Critical: Sudden large spike in aFRR Up by {aFRR_spike_up} MWh at {df.index[i]}"
            alarms.append(message)

        if aFRR_spike_down >= AFRR_SPIKE_THRESHOLD:
            message = f"🚨 Critical: Sudden large spike in aFRR Down by {aFRR_spike_down} MWh at {df.index[i]}"
            alarms.append(message)

    # Play sound only if new alarms are detected
    if alarms and len(alarms) > st.session_state["last_alarm_count"]:
        st.session_state["last_alarm_count"] = len(alarms)
        alarm_triggered = True

    if alarm_triggered:
        make_test_call()

    return alarms

# Layout for the app with columns
col1, col2 = st.columns([2, 1])  # Table takes 2/3 width, alarms take 1/3 width

with col1:
    st.subheader("Activation Energy Table")
    data = fetch_balancing_energy_data()
    if not data.empty:
        st.write(data)
    else:
        st.warning("No data available.")

with col2:
    st.subheader("Alarms Triggered")
    alarm_list = check_balancing_alarms(data)
    if alarm_list:
        for alarm in alarm_list[::-1]:  # Display most recent alarms first
            st.warning(alarm)
    else:
        st.success("✅ No alarms triggered.")

# Auto-refresh every 5 minutes
for seconds_left in range(60, 0, -1):  # 300 seconds = 5 minutes
    time.sleep(1)  # Countdown with a 1-second delay

# Automatically rerun the app
st.rerun()
