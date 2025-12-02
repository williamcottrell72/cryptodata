#!/usr/bin/env python3
"""Quick script to fetch available query fields from a subgraph."""
import os
import sys
import requests

def main():
    api_key = os.environ.get('GRAPH_API_KEY')
    if not api_key:
        print("Error: GRAPH_API_KEY not set")
        sys.exit(1)

    subgraph_id = sys.argv[1] if len(sys.argv) > 1 else "9JLB7VbhJaGRtiFVvA6b4vDDwsfWF5rbY8Gd3zAUW1T7"

    url = f'https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}'

    # Get all query fields
    query = '{ __schema { queryType { fields { name } } } }'
    response = requests.post(url, json={'query': query})
    data = response.json()

    if 'errors' in data:
        print(f'Errors: {data["errors"]}')
        sys.exit(1)

    fields = data.get('data', {}).get('__schema', {}).get('queryType', {}).get('fields', [])
    print('Available query fields:')
    for f in sorted(fields, key=lambda x: x['name']):
        print(f'  {f["name"]}')

if __name__ == "__main__":
    main()
