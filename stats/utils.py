import json
import requests
from django.conf import settings

def slack_latest(text, channel):
    endpoints = {
        '#covid-tracking': settings.SLACK_WEBHOOK_ENDPOINT_COVID_TRACKING,
        '#virus': settings.SLACK_WEBHOOK_ENDPOINT_VIRUS,
        '#robot-dojo': settings.SLACK_WEBHOOK_ENDPOINT_DOJO,
    }
    # endpoint = settings.SLACK_WEBHOOK_ENDPOINT
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
    r = requests.post(endpoints[channel], data=json.dumps(payload), headers=headers)
