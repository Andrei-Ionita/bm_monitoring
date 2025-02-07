import streamlit as st
import base64

# Page configuration for wide layout
st.set_page_config(layout="wide")

# Function to convert alarm.mp3 to base64
def get_base64_audio(file_path):
    with open(file_path, "rb") as audio_file:
        return base64.b64encode(audio_file.read()).decode()

# Load and encode alarm.mp3 to base64
audio_base64 = get_base64_audio("alarm.mp3")

# Initialize session state for tracking alarms
if "alarm_triggered" not in st.session_state:
    st.session_state["alarm_triggered"] = False

# HTML and JavaScript to handle audio playback with retry logic
html_code = f"""
<audio id="alarmAudio" loop preload="auto">
    <source src="data:audio/mpeg;base64,{audio_base64}" type="audio/mpeg">
    Your browser does not support the audio element.
</audio>

<script>
    // Play the alarm sound
    function playAlarm() {{
        var audio = document.getElementById('alarmAudio');
        
        // Attempt playback and retry on failure
        function attemptPlay() {{
            audio.play().then(function() {{
                console.log('Alarm playing.');
            }}).catch(function(error) {{
                console.log('Playback failed. Retrying in 1s...', error);
                setTimeout(attemptPlay, 1000);  // Retry every second until it plays
            }});
        }}

        attemptPlay();  // Initial attempt
    }}

    // Automatically trigger sound if alarm_triggered is set
    if ({str(st.session_state["alarm_triggered"]).lower()}) {{
        playAlarm();
    }}
</script>
"""

# Display HTML and JS for the audio
st.components.v1.html(html_code, height=0)

# UI for triggering and stopping the alarm
st.title("Balancing Energy Monitoring with Persistent Alarm")

if st.button("Trigger Alarm"):
    st.session_state["alarm_triggered"] = True
    st.warning("🚨 Alarm Triggered!")

if st.button("Stop Alarm"):
    st.session_state["alarm_triggered"] = False
    st.success("Alarm Stopped.")
