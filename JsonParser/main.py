#!/usr/bin/python
# -*- encoding: utf-8 -*-

import json
from customDataFilterer import DataFilterer


def parse_json_file(file_path=""):
    with open(file_path, "r", encoding="utf-8") as m_file:
            return json.load(m_file)

def main():
    try:
        # parse data from file
        data = parse_json_file("DataFolder/take-home.json").get("prices")
        
        # create filter dictionary with target and rule options
        filter_dictionary = {  
            "name": "Last tag symbol must be \'1\'",
            "target": "tag",
            "rule": lambda val: val[-1] == "1",
        }

        """ 
        create custom class object
        with part of data 'prices' and filter dictionary
        """
        data_filter = DataFilterer(data, filter_dictionary)

        # get filtered prices and filter
        filtered_data_prices, allowed_filter = data_filter.get_filtered  
        
        # show filtered prices
        print(filtered_data_prices)  
        # show filter
        print(allowed_filter.get('name'))  

    except Exception as _ex:
        print(_ex)


if __name__ == "__main__":
    main()
