import json

def parse_json_file(file_path=''):
    try:
        with open(file_path, 'r', encoding='utf-8') as m_file:
            return json.load(m_file)
    except Exception as _ex:
        print(_ex)
        return {}

def get_filtered_data(data={}, filter={}):
    return (filter(lambda val: val[-1]=='1', data))

def main():
    data = parse_json_file('DataFolder/take-home.json')
    filter_example = {}
    filtered_data = get_filtered_data(data.get('prices'))
    print(filtered_data)
if __name__ == "__main__":
    main()