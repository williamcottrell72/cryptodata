#!/usr/bin/env python3
"""Quick script to fetch fields of a specific entity from a subgraph."""
import os
import sys
import json
import requests

def main():
    api_key = os.environ.get('GRAPH_API_KEY')
    if not api_key:
        print("Error: GRAPH_API_KEY not set")
        sys.exit(1)

    entity_name = sys.argv[1] if len(sys.argv) > 1 else "Liquidation"
    subgraph_id = sys.argv[2] if len(sys.argv) > 2 else "9JLB7VbhJaGRtiFVvA6b4vDDwsfWF5rbY8Gd3zAUW1T7"

    url = f'https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}'

    # Get fields for specific type
    query = f'''
    {{
      __type(name: "{entity_name}") {{
        name
        fields {{
          name
          type {{
            name
            kind
            ofType {{
              name
              kind
            }}
          }}
        }}
      }}
    }}
    '''
    response = requests.post(url, json={'query': query})
    data = response.json()

    if 'errors' in data:
        print(f'Errors: {data["errors"]}')
        sys.exit(1)

    type_info = data.get('data', {}).get('__type')
    if not type_info:
        print(f'Type "{entity_name}" not found')
        sys.exit(1)

    print(f'Fields for {type_info["name"]}:')
    for field in type_info.get('fields', []):
        field_type = field['type']
        type_name = field_type.get('name') or field_type.get('kind')
        if field_type.get('ofType'):
            inner = field_type['ofType']
            type_name = f"{field_type['kind']}<{inner.get('name') or inner.get('kind')}>"
        print(f'  {field["name"]}: {type_name}')

if __name__ == "__main__":
    main()
