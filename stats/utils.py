import json
import math
import requests
from django.conf import settings

def slack_latest(text, channel):
    MAX_BLOCK_LENGTH = 3000

    endpoints = {
        '#covid-tracking': settings.SLACK_WEBHOOK_ENDPOINT_COVID_TRACKING,
        '#virus': settings.SLACK_WEBHOOK_ENDPOINT_VIRUS,
        '#robot-dojo': settings.SLACK_WEBHOOK_ENDPOINT_DOJO,
    }
    # endpoint = settings.SLACK_WEBHOOK_ENDPOINT
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
    }

    char_count = len(text)
    remaining_text = text
    if char_count <= MAX_BLOCK_LENGTH:
        block_texts = [text]
    else:
        block_texts = []
        n = 0
        how_many_times = math.ceil(char_count / MAX_BLOCK_LENGTH)
        while n < how_many_times:
            raw_substring = remaining_text[0:MAX_BLOCK_LENGTH]
            last_line_break_charnum = raw_substring.rfind('\n')
            clean_substring = remaining_text[0:last_line_break_charnum]
            block_texts.append(clean_substring.strip())
            remaining_text = remaining_text[last_line_break_charnum:]
            n+=1

        # Check for remainder
        if len(remaining_text.strip()) > 0:
            block_texts.append(remaining_text)


    blocks_formatted_list = [{
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": b
        }
    } for b in block_texts]

    payload = {
        # 'text': text,
        'blocks': blocks_formatted_list
    }

    print(payload)
    r = requests.post(endpoints[channel], data=json.dumps(payload), headers=headers)

    if r.ok:
        print('Slack message appears to have fired succesfully.')
    else:
        print('Slack error...')
        print(r.status_code)
        print(r.text)
