#!/usr/bin/python
# -*- encoding: utf-8 -*-

class DataFilterer:
    r"""The DataFilterer class is used for custom filtering data.
   The main application is parsing a list with dictionaries using a filter.
   
   Attributes
   ------------
   data : list of dict
        list with dictionaries to process
   filter_prop : dict
        dictionary with target -> string 
        and data filtering rule -> lambda/function/delegate
   
   Methods
   ----------
   check_params(filter_options = None)
        Checks the parameters of an object and the specified parameter.
   get_filtered(filter_options = None)
        Applies the passed or custom filter to the data it returns.
   get_data
        Returns the data in its original state.
   set_data(data)
        Passes data to the instance.
   get_filter
        Returns the instance's filter
   set_filter(filter_prop)
        Passes an instance filter
   """
    ### Ctor
    def __init__(self, data:list, filter_prop:dict):
        self.__data = data
        self.check_params(filter_prop)
        self.__filter_prop = filter_prop
    ### Methods
    def check_params(self, filter_options = None):
        r"""Method for checking set and passed parameters
        Raises
        -------
        ValueError
            If the parameters do not match the requirements, 
            an exception will be raised with the message what went wrong
        """
        if not isinstance(self.get_data, list):
            raise ValueError(f'The passed parameter {type(self.get_data)}.\ndoes not match the {type(list())} of {type(dict())} layout.')
        for item in self._data:
            if not isinstance(item,dict):
                raise ValueError(f'The passed parameter {type(self.get_data)}.\ndoes not match the {type(list())} of {type(dict())} layout.')
        if not isinstance(self.get_filter, dict):
            raise ValueError(
                f'The passed parameter {type(self.get_filter)}.'\
                f'\ndoes not match the {type(dict())} layout.'
                )
        if not set(['target', 'rule']).issubset(self.get_filter.keys()):
            raise ValueError(
                    'Passed filter is missing required parameters:\n'\
                    f'{"target" if "target" not in self.get_filter else ""} '\
                    f'{"values" if "values" not in self.get_filter else ""}.'
                )
        if filter_options is not None:
            if not isinstance(filter_options, dict):
                raise ValueError(
                    f'The passed parameter {type(filter_options)}.'\
                    f'\ndoes not match the {type(dict())} layout.'
                    )
            if not set(['target', 'rule']).issubset(self.filter_options.keys()):
                raise ValueError(
                        'Passed filter is missing required parameters:\n'\
                        f'{"target" if "target" not in filter_options else ""} '\
                        f'{"values" if "values" not in filter_options else ""}.'
                    )

    ### Properties
    @property
    def get_filtered(self, filter_options = None):
        """
        Here we have a parameter that the user can pass to the method.
        We have to check it and in case of error we will return 
        the filtered data with our (internal) checked parameter.
        After all, we must return a filtered list with the correct filter anyway.
        """
        if filter_options is not None:
            try:
                self.check_params(filter_options)
                return list( 
                    filter ( 
                        lambda data : filter_options.get('rule')(   data.get(   filter_options.get('target')    )   ), self.get_data 
                        ) 
                    ) , filter_options
            except Exception as ex:
                return list(
                   filter (
                      lambda data : self.get_filter.get('rule')(   data.get(   self.get_filter.get('target')  )   ), self.get_data 
                      ) 
                   ) , self.get_filter
        else:
            return list(
               filter (
                  lambda data : self.get_filter.get('rule')(   data.get(   self.get_filter.get('target')  )   ), self.get_data 
                  )
              ), self.get_filter

    @property
    def get_data(self):
        '''returns full data as list of dicts'''
        return self.__data

    @property
    def set_data(self, data):
        '''sets data'''
        self.__data = data

    @property
    def get_filter(self):
        """Return the filter -> dict."""
        return self.__filter_prop

    @property
    def set_filter(self, filter_prop):
        self.check_params(filter_prop)
        self.__filter_prop = filter_prop
