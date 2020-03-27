import json
import requests
from django.conf import settings

def slack_latest(text):
    endpoint = settings.SLACK_WEBHOOK_ENDPOINT
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
    }
    payload = {
        # 'text': text,
        'blocks': [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": text
                }
            }
        ]
    }
    print(payload)
    r = requests.post(endpoint, data=json.dumps(payload), headers=headers)
