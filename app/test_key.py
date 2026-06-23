'''
It's a script with no FastAPI, no routes, no web server — nothing wrapped in your app.
It does exactly one thing: sends a single hardcoded message to Groq's API using your key, and prints whatever comes back.
'''
import os
from dotenv import load_dotenv
from groq import Groq

load_dotenv() # .env file actually gets read

client = Groq(api_key=os.environ["GROQ_API_KEY"])

# Talks to internet -> sends a http request to Groq's servers and waits for a reply
# model -> which model to run the prompt on
# messages, role -> who's speaking ("user", "assistant","system")
response = client.chat.completions.create(
    model="llama-3.1-8b-instant",
    messages=[{"role": "user", "content": "Say hello in 5 words"}]
)

print(response)      

# complex object -> not just a string - containing metadata, token usage, model's reply and more
print(response.choices[0].message.content)
