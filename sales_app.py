import os
import requests
from flask import Flask, request, jsonify
import cohere

app = Flask(__name__)

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "sales_secret_token")
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
MY_NOTIFY_PHONE = os.getenv("MY_NOTIFY_PHONE", "917500058655")  # Aapka personal number jahan meeting alert aayega

co = cohere.ClientV2(api_key=COHERE_API_KEY)
PROCESSED_MESSAGES = set()
LEAD_CHATS = {}

ACTIVE_MODEL = "command-r-08-2024"

def load_pitch_data():
    try:
        with open("pitch_data.txt", "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        print(f"Error reading pitch_data.txt: {e}")
        return "AI Receptionist for Hotels. Contact: +91-7500058655"

def mark_message_as_read(message_id):
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "status": "read",
        "message_id": message_id
    }
    try:
        requests.post(url, headers=headers, json=payload, timeout=2)
    except Exception as e:
        print(f"Read receipt error: {e}")

def send_whatsapp_message(to_number, message_text):
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "text",
        "text": {"body": message_text}
    }
    try:
        requests.post(url, headers=headers, json=payload, timeout=5)
    except Exception as e:
        print(f"Send Error: {e}")

def get_ai_reply(sender_phone, user_message):
    pitch_context = load_pitch_data()
    
    preamble = f"""
Aapka naam 'Kabir' hai, aap Hotel Automation & AI Solutions ke Business Development Executive hain.
Aap Haridwar, Rishikesh aur Uttarakhand ke Hotel Owners aur Managers se baat kar rahe hain.

MAQSAD:
Hotel owner ko solution samjhana aur unke sath ek quick 10-minute demo call ya direct meeting fix karna.

RULES:
1. Tone: Respectful, sharp, professional business Hinglish ('Namaste sir', 'Ji bilkul').
2. Length: Max 2-3 short impactful lines. Lamba lecture mat dein.
3. OTA Commission & 24/7 Response: Hamesha highlight karein ki kaise ye MakeMyTrip ka 20% commission bachata hai aur raat ki missed bookings pakadta hai.
4. Pricing puchne par: Bolein "Sir, setup bohot pocket-friendly hai, basic staff ki salary se bhi kaafi kam. Main pehle aapke hotel ke naam ka 2-minute live demo dikhana chahta hu. Kal kis time baat ho sakti hai?"
5. MEETING/DEMO CONFIRMATION:
   Agar owner bole 'theek hai demo dikhao', 'call karo', ya time bataye (jaise 'kal 11 baje'), toh aakhri line me exact yeh secret tag lagayein:
   [MEETING_FIXED: Hotel Owner Phone <phone> - Timing <time_details>]
   Aur reply dein: "Ji done sir! Humari team aapko scheduled time par contact karegi aur live demo dikhayegi. Thank you!"

KNOWLEDGE BASE:
{pitch_context}
"""
    if sender_phone not in LEAD_CHATS:
        LEAD_CHATS[sender_phone] = []

    history = LEAD_CHATS[sender_phone]
    history.append({"role": "user", "content": user_message})

    messages_payload = [{"role": "system", "content": preamble}] + history[-6:]

    try:
        response = co.chat(
            model=ACTIVE_MODEL,
            messages=messages_payload,
            temperature=0.2
        )
        reply = response.message.content[0].text.strip()

        if "[MEETING_FIXED:" in reply:
            meeting_details = reply.split("[MEETING_FIXED:")[1].split("]")[0].strip()
            reply = reply.split("[MEETING_FIXED:")[0].strip()
            
            alert = f"🔥 *Nayi Hotel Lead / Demo Fixed!*\nDetails: {meeting_details}\nLead Phone: +{sender_phone}"
            send_whatsapp_message(MY_NOTIFY_PHONE, alert)

        history.append({"role": "assistant", "content": reply})
        if len(history) > 10:
            LEAD_CHATS[sender_phone] = history[-6:]

        return reply
    except Exception as e:
        print(f"--- COHERE ERROR: {e} ---")
        return "Namaste sir! Main Kabir, Hotel Automation se. Batayein aapke hotel ke liye live AI demo kab schedule karein?"

@app.route("/webhook", methods=["GET"])
def verify_webhook():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200
    return "Forbidden", 403

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()
    try:
        if data.get("entry"):
            for entry in data["entry"]:
                for change in entry.get("changes", []):
                    value = change.get("value", {})
                    if "messages" in value:
                        message = value["messages"][0]
                        msg_id = message.get("id")
                        sender_phone = message.get("from")

                        if msg_id in PROCESSED_MESSAGES:
                            return jsonify({"status": "already_processed"}), 200
                        PROCESSED_MESSAGES.add(msg_id)

                        if len(PROCESSED_MESSAGES) > 500:
                            PROCESSED_MESSAGES.clear()

                        if message.get("type") == "text":
                            incoming_text = message["text"]["body"]
                            mark_message_as_read(msg_id)
                            reply_text = get_ai_reply(sender_phone, incoming_text)
                            send_whatsapp_message(sender_phone, reply_text)
    except Exception as err:
        print(f"Webhook Error: {err}")

    return jsonify({"status": "success"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
