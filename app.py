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
                print(f"Skipping {eet_from} - before today’s midnight")
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

# Function to detect and handle alarms
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
            if message not in critical_alarms:
                critical_alarms.append(message)
                all_alarms.append((df.index[i], message, "Critical"))

    else:
        message = "🚨 Critical: No data available from the server."
        if message not in critical_alarms:
            critical_alarms.append(message)
            all_alarms.append((df.index[i], message, "Critical"))

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
                all_alarms.append((df.index[i], message, "Critical"))

        if previous_total_down > previous_total_up and current_total_up > current_total_down:
            message = (f"🚨 Critical: System switched from downward total activation to upward "
                       f"total activation at {df.index[i]}")
            if message not in critical_alarms:
                print(f"Triggering Critical Alarm: {message}")
                critical_alarms.append(message)
                all_alarms.append((df.index[i], message, "Critical"))

        # ================= Warning Alarms ============================================
        if current_mFRR_up < previous_mFRR_up and current_aFRR_down > previous_aFRR_down:
            message = f"⚠️ Warning: mFRR Up decreasing and aFRR Down increasing at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                all_alarms.append((df.index[i], message, "Warning"))

        if current_mFRR_down < previous_mFRR_down and current_aFRR_up > previous_aFRR_up:
            message = f"⚠️ Warning: mFRR Down decreasing and aFRR Up increasing at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                all_alarms.append((df.index[i], message, "Warning"))

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
                all_alarms.append((df.index[i], message, "Warning"))

        # Check for sudden increase or drop in mFRR Down
        if abs(rate_of_change_down) >= RATE_OF_CHANGE_THRESHOLD:
            if rate_of_change_down > 0:
                message = f"⚠️ Warning: Sudden increase in mFRR Down by {rate_of_change_down} MWh at {df.index[i]}"
            else:
                message = f"⚠️ Warning: Sudden drop in mFRR Down by {abs(rate_of_change_down)} MWh at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                all_alarms.append((df.index[i], message, "Warning"))


        # ================= Critical Alarms ====================================
        if previous_mFRR_up > 0 and current_mFRR_down > 0:
            message = f"🚨 Critical: System switched from deficit to surplus at {df.index[i]}"
            if message not in critical_alarms:
                critical_alarms.append(message)
                all_alarms.append((df.index[i], message, "Critical"))

        if previous_mFRR_down > 0 and current_mFRR_up > 0:
            message = f"🚨 Critical: System switched from surplus to deficit at {df.index[i]}"
            if message not in critical_alarms:
                critical_alarms.append(message)
                all_alarms.append((df.index[i], message, "Critical"))

        # Opposite aFRR activation (aFRR spike in opposite direction)
        if previous_aFRR_up > previous_aFRR_down and previous_aFRR_up > THRESHOLD_AFRR_UP:
            if current_aFRR_down > previous_aFRR_down and current_aFRR_down > THRESHOLD_AFRR_DOWN:
                message = f"🚨 Critical: Sudden spike in aFRR Down at {df.index[i]}"
                if message not in critical_alarms:
                    critical_alarms.append(message)
                    all_alarms.append((df.index[i], message, "Critical"))

        if previous_aFRR_down > previous_aFRR_up and previous_aFRR_down > THRESHOLD_AFRR_DOWN:
            if current_aFRR_up > previous_aFRR_up and current_aFRR_up > THRESHOLD_AFRR_UP:
                message = f"🚨 Critical: Sudden spike in aFRR Up at {df.index[i]}"
                if message not in critical_alarms:
                    critical_alarms.append(message)
                    all_alarms.append((df.index[i], message, "Critical"))

          # ================= aFRR-Only Imbalance Detection ===========================

        # aFRR-only dominance and system direction shift
        if previous_aFRR_up > previous_aFRR_down and current_aFRR_down > current_aFRR_up:
            message = f"⚠️ Warning: aFRR switched from Up to Down dominance at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                all_alarms.append((df.index[i], message, "Warning"))

        if previous_aFRR_down > previous_aFRR_up and current_aFRR_up > current_aFRR_down:
            message = f"⚠️ Warning: aFRR switched from Down to Up dominance at {df.index[i]}"
            if message not in warning_alarms:
                warning_alarms.append(message)
                all_alarms.append((df.index[i], message, "Warning"))

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

                # Prevent duplicates
                if message not in critical_alarms:
                    critical_alarms.append(message)
                    all_alarms.append((df.index[-1], message, "Critical"))
                    print(f"Triggering Critical Alarm: {message}")


        # Large spikes in aFRR
        aFRR_spike_up = abs(current_aFRR_up - previous_aFRR_up)
        aFRR_spike_down = abs(current_aFRR_down - previous_aFRR_down)

        if aFRR_spike_up >= AFRR_SPIKE_THRESHOLD:
            message = f"🚨 Critical: Sudden large spike in aFRR Up by {aFRR_spike_up} MWh at {df.index[i]}"
            if message not in critical_alarms:
                critical_alarms.append(message)
                all_alarms.append((df.index[i], message, "Critical"))

        if aFRR_spike_down >= AFRR_SPIKE_THRESHOLD:
            message = f"🚨 Critical: Sudden large spike in aFRR Down by {aFRR_spike_down} MWh at {df.index[i]}"
            if message not in critical_alarms:
                critical_alarms.append(message)
                all_alarms.append((df.index[i], message, "Critical"))

        # Check for new critical alarms and make a call if any
        new_critical_alarms = [alarm for alarm in critical_alarms if alarm not in st.session_state["calls_made_critical"]]
        if new_critical_alarms:
            for alarm in new_critical_alarms:
                if is_valid_phone_number(USER_PHONE_NUMBER):
                    alarm_id = df.index[-1]  # Ensure an alarm ID is passed
                    make_call("Critical", alarm, alarm_id)
                    st.session_state["calls_made_critical"].append(alarm)  # Store it immediately

        # Check for new warning alarms and make a call if not during night hours
        if not is_night_time():
            new_warning_alarms = [alarm for alarm in warning_alarms if alarm not in st.session_state["calls_made_warning"]]
            if new_warning_alarms:
                for alarm in new_warning_alarms:
                    if is_valid_phone_number(USER_PHONE_NUMBER):
                        alarm_id = df.index[-1]
                        make_call("Warning", alarm, alarm_id)
                        st.session_state["calls_made_warning"].append(alarm)

    # Append only new alarms to the stored session state
    for alarm in all_alarms:
        if alarm not in st.session_state["all_alarms"]:
            st.session_state["all_alarms"].append(alarm)

    # Return stored alarms to display in UI
    return st.session_state["all_alarms"]

# Define the EET timezone
eet_timezone = pytz.timezone('Europe/Bucharest')

# Layout for the app with columns
col1, col2 = st.columns([2, 1])  # Table takes 2/3 width, alarms take 1/3 width

with col1:
    st.subheader("Activation Energy Table")
    # Display current time and update info
    # Get the current time in EET for the "Last updated" information
    current_time_eet = datetime.now().astimezone(eet_timezone).strftime("%Y-%m-%d %H:%M:%S")
    st.info(f"Last updated: **{current_time_eet}**")
    data = fetch_balancing_energy_data()
    if not data.empty:
        st.write(data)
    else:
        st.warning("No data available.")

with col2:
    st.subheader("Alarms Triggered")
    all_alarms = check_balancing_alarms(data)

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
asyncio.run(refresh_app(60))  # Refresh every 60 seconds
