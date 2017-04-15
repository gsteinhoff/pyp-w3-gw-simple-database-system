import os
import json
from datetime import date

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH


class RowModel(object):
    def __init__(self, row):
        for key, value in row.items():
            setattr(self, key, value)


class Table(object):

    def __init__(self, db, name, columns=None):
        self.db = db
        self.name = name

        self.table_filepath = os.path.join(BASE_DB_FILE_PATH, self.db.name,
                                           '{}.json'.format(self.name))

        # In case the table JSON file doesn't exist already, you must
        # initialize it as an empty table, with this JSON structure:
        # {'columns': columns, 'rows': []}
        if not os.path.exists(self.table_filepath):
            with open(self.table_filepath, 'w+') as tb_file:
                default_table = { 'columns': columns, 'rows': [] }
                tb_file.write(json.dumps(default_table))
                
        self.columns = columns or self._read_columns()
        
        #object to hold query results that can be parsed
        self.RowObj = type(self.name.title(), (RowModel, ), {})

    def _read_columns(self):
        # Read the columns configuration from the table's JSON file
        # and return it.
        with open(self.table_filepath) as tb_file:
            output = json.load(tb_file)['columns']
        return output

    def _validate_row(self, *args):
        # If we don't have matching num of values for columns
        if len(args) != len(self.columns):
            return False, 'Invalid amount of fields.'     
        # put the two lists into one with value and column object
        for arg_value, column_value in zip(args, self.columns):
            # if value doesn't match type of column then not valid
            if not isinstance(arg_value, eval(column_value['type'])):
                # create error message as formated in tests
                msg = ('Invalid type of field "{}": Given "{}", expected "{}"'
                        ''.format(column_value['name'], type(arg_value).__name__, 
                        eval(column_value['type']).__name__))
                return False, msg
        # All args match length and types of columns so return that it is valid
        return True, None
        
    def _format_row(self, *args):
        row = {}
        # build name list
        name_list = [col['name'] for col in self.columns]
        # build column to value associated list
        for col_name, field_value in zip(name_list, args):
            if (type(field_value).__name__ == 'date'):
                row[col_name] = field_value.isoformat()
            else:
                row[col_name] = field_value
        return row
                
    def _insert_row(self, row):
        # open file
        with open(self.table_filepath, 'r+') as tb_file:
            data = json.load(tb_file)
            data['rows'].append(row)
            tb_file.seek(0)
            tb_file.write(json.dumps(data, indent=4))
        return True

    def insert(self, *args):
        # Validate that the provided row data is correct according to the
        # columns configuration.
        # If there's any error, raise ValidationError exception.
        # Otherwise, serialize the row as a string, and write to to the
        # table's JSON file.
        valid, msg = self._validate_row(*args)
        if not valid:
            raise ValidationError(msg)
        row = self._format_row(*args)
        return self._insert_row(row)
        

    def query(self, **kwargs):
        # Read from the table's JSON file all the rows in the current table
        # and return only the ones that match with provided arguments.
        # We would recomment to  use the `yield` statement, so the resulting
        # iterable object is a generator.

        # IMPORTANT: Each of the rows returned in each loop of the generator
        # must be an instance of the `Row` class, which contains all columns
        # as attributes of the object.
        with open(self.table_filepath) as tb_file:
            rows = json.load(tb_file)['rows']
            for row in rows:
                if not all(row.get(key) == value for key, value in kwargs.items()):
                    continue
                yield self.RowObj(row)
                

    def all(self):
        # Similar to the `query` method, but simply returning all rows in
        # the table.
        # Again, each element must be an instance of the `Row` class, with
        # the proper dynamic attributes.
        with open(self.table_filepath) as tb_file:
            rows = json.load(tb_file)['rows']
            for row in rows:
                yield self.RowObj(row)
            

    def count(self):
        # Read the JSON file and return the counter of rows in the table
        with open(self.table_filepath) as tb_file:
            return len(json.load(tb_file)['rows'])
            

    def describe(self):
        # Read the columns configuration from the JSON file, and return it.
        return self.columns
    
    


class DataBase(object):
    def __init__(self, name):
        self.name = name
        self.db_filepath = os.path.join(BASE_DB_FILE_PATH, self.name)
        self.tables = self._read_tables()

    @classmethod
    def create(cls, name):
        db_filepath = os.path.join(BASE_DB_FILE_PATH, name)
        # if the db directory already exists, raise ValidationError
        # otherwise, create the proper db directory
        if os.path.exists(db_filepath):
            msg = ('Database with name "{}" already exists.'.format(name))
            raise ValidationError(msg)
        os.makedirs(db_filepath)

    def _read_tables(self):
        # Gather the list of tables in the db directory looking for all files
        # with .json extension.
        # For each of them, instatiate an object of the class `Table` and
        # dynamically assign it to the current `DataBase` object.
        # Finally return the list of table names.
        # Hint: You can use `os.listdir(self.db_filepath)` to loop through
        #       all files in the db directory
        tb_names = [file.replace('.json', '') for file in os.listdir(self.db_filepath)]
        for name in tb_names:
            setattr(self, name, Table(db=self, name=name))
        return tb_names

    def create_table(self, table_name, columns):
        # Check if a table already exists with given name. If so, raise
        # ValidationError exception.
        # Otherwise, crete an instance of the `Table` class and assign
        # it to the current db object.
        # Make sure to also append it to `self.tables`
        if table_name in self.tables:
            msg = ('Table with name "{}" in DB "{}" '
                    'already exist.'.format(table_name, self.name))
            raise ValidationError(msg)
        # build new table in this database
        new_table = Table(db=self, name=table_name, columns=columns)
        # append the new table to the tables list
        self.tables.append(table_name)
        # add the table object by name to this database
        setattr(self, table_name, new_table)
        return True
        

    def show_tables(self):
        # Return the curren list of tables.
        return self.tables


def create_database(db_name):
    """
    Creates a new DataBase object and returns the connection object
    to the brand new database.
    """
    DataBase.create(db_name)
    return connect_database(db_name)


def connect_database(db_name):
    """
    Connectes to an existing database, and returns the connection object.
    """
    return DataBase(name=db_name)
