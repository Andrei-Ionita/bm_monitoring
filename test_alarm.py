import streamlit as st
import base64

# Convert alarm.mp3 to base64
def get_base64_audio(audio_file):
    with open(audio_file, "rb") as file:
        encoded_string = base64.b64encode(file.read()).decode()
    return encoded_string

# Load the alarm audio as base64
audio_base64 = get_base64_audio("alarm.mp3")

# HTML + JS for delayed and reliable playback
html_code = f"""
<audio id="alarmAudio" preload="auto">
    <source src="data:audio/mpeg;base64,{audio_base64}" type="audio/mpeg">
    Your browser does not support the audio element.
</audio>

<script>
    document.addEventListener("DOMContentLoaded", function() {{
        setTimeout(function() {{
            var audio = document.getElementById('alarmAudio');
            audio.currentTime = 0;
            audio.play().then(() => {{
                console.log("Audio played successfully");
            }}).catch(error => {{
                console.error("Audio playback failed:", error);
                alert('Autoplay blocked! Please enable autoplay or click the button below.');
            }});
        }}, 1000);  // 1-second delay after DOM is loaded
    }});
</script>
"""

# Display in Streamlit
st.components.v1.html(html_code, height=100)
