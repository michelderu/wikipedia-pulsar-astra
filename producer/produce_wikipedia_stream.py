import json
import requests
import pulsar
from dotenv import load_dotenv
import os
import datetime
from datetime import timezone
import sys

load_dotenv()

def get_pulsar_producer():
    service_url = os.getenv('PULSAR_SERVICE')
    token = os.getenv('PULSAR_TOKEN')
    client = pulsar.Client(service_url, authentication=pulsar.AuthenticationToken(token))
    return client, client.create_producer(os.getenv('PULSAR_TOPIC'))

client, producer = get_pulsar_producer()

def read_wiki_extract(title: str):
    response = requests.get(
        'https://en.wikipedia.org/w/api.php',
        params={
            'action': 'query',
            'format': 'json',
            'titles': title,
            'prop': 'extracts',
            'exintro': True,
            'explaintext': True,
        }).json()
    page = next(iter(response['query']['pages'].values()))
    return page['extract']

def publish_extract(extract: str):
    producer.send((extract).encode('utf-8'))

if len(sys.argv) > 1:
    messages = int(sys.argv[1])
    print(f"Number of messages: {messages}")
else:
    print("publishWikipediaStream.py <number of messages>")
    exit(1)

r = requests.get('https://stream.wikimedia.org/v2/stream/recentchange', stream=True)
if r.encoding is None:
    r.encoding = 'utf-8'
count = 0
for line in r.iter_lines(decode_unicode=True):
    if count >= messages:
        break
    if line and line.startswith('data'):
        update = json.loads(line[6:])
        title = update['title'] if update['title'] else ''
        type = update['type'] if update['type'] else ''
        url = update['title_url'] if update['title_url'] else ''
        servername = update['server_name'] if update['server_name'] else ''
        if type == 'edit' and servername == 'en.wikipedia.org' and ':' not in title:
            print(f"[{type} @ {servername}] {title} - {url}")
            extract = read_wiki_extract(title)
            timestamp = f"{datetime.datetime.now(timezone.utc).isoformat()[:-3]}Z"
            # Langflow compatible (after vectorization) as title and source are used as metadata and content as the body
            extract_json = {
                'title': title,
                'source': url,
                'content': extract,
                'timestamp': timestamp,
                'date': timestamp.split('T')[0]
            }
            publish_extract(json.dumps(extract_json))
            count += 1

print(f"Published {count} messages")
client.close()