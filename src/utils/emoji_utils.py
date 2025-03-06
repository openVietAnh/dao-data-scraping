import re

def remove_emojis(text):
    return re.sub(r'[^\x00-\x7F]+', '', text) if text else text