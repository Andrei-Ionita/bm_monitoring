from twilio.rest import Client
from dotenv import load_dotenv
import os

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

# Run the test call
if __name__ == "__main__":
    make_test_call()
