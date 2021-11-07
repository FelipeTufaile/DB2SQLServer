## excel2sqlserver converter funtion:
def excel2sqlserver(table_path, table_name, columns_format, credentials, mode = 'append'):

    """
    Input:
    1 - table_path: path of the table that has to be loaded into a database    | string
    2 - table_name: name of the table in the database                          | string
    3 - columns_format: information about column's types                       | dictionary
    4 - credentials: credentials information for given access to the database  | dictionary
    5 - mode: mode used to created / update table                              | string

    Output:
    1 - Succes/ Failure notification.
    """

    #import libraries
    import pyodbc
    import pandas as pd
    import re
    from time import sleep
    import tqdm

    ## GET TABLE'S IFORMATION
    ##########################################################################################################################################
    # read table
    tb = pd.read_excel(table_path, sheet_name = 'Sheet1', header = 0)

    # get table information
    columns_list = list(columns_format.keys())
    ncolumns = len(columns_list)

    ## FUNCTIONS DEFINITION
    ##########################################################################################################################################

    def format_value(value, column, column_type):
        
        """
        Input: 1- value: receives an entry from the table
                2 - columns format: definition of column type
        Output: 1- return the entry formtted according to its column definition
        """

        # import libraries
        import numpy as np
    
        try:
                # check if it is a numpy 'nan' value
                if(np.isnan(value)):
                        return ''

                # verify if it is decimal
                elif('REAL' in column_type):
                        return float(value)

                # verify if it is integer
                elif('INT' in column_type):
                        return int(value)
                
        except TypeError:
                    
                # verify if it is null
                if(value == None):
                        return ''
                
                # verify if it is string
                elif('VARCHAR' in column_type):
                        value = str(value)
                        value = value.replace("'", "")
                        value = value.replace('"', '')
                        value = value.replace("None", "")
                        value = value.replace("\\r", "")
                        value = value.replace('\\n', '')
                        value = value.replace('\\', '')
                        return value

    ## START CONNECTION
    ##########################################################################################################################################

    # read credentials
    driver = '{ODBC Driver 17 for SQL Server}'
    server = credentials['server']
    database = credentials['database']
    username = credentials['username']
    password = credentials['password']

    # create connection string
    connection_string = 'Driver={driver};Server=tcp:{server},1433;Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    # start connection using pyodbc
    cnxn = pyodbc.connect(connection_string.format(driver = driver, server = server, database = database, username = username, password = password))
    
    # create cursor
    cursor = cnxn.cursor()

    ## INITIALIZE TABLE
    ##########################################################################################################################################

    # there are two types of mode 'overwrite' and 'append'. Append is the default and it appends the information (new entries) on an existing
    # table. Overwrite is an option that has to be set in 'mode' argument when calling 'excel2sqlserver' function. It overwrites an existing table
    # if it already exists or it creates a new table with a given name (also given as a function argument).

    if(mode == 'overwrite'):
        # DROP TABLE IF EXISTS
        drop_table = "IF OBJECT_ID (N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}".format(table = table_name)
        cursor.execute(drop_table)

        # initialize create table statement
        create_table = 'CREATE TABLE dbo.{table} ({tables_definitions})'

        # initialize column counter
        n = 1

        # initialize columns definitions statements
        tables_definitions = ''

        # iterate through all the columns
        for item in columns_format:
        
            # check wether it is the last columns
            if(n < ncolumns):
                column_line = str(item) + ' ' + str(columns_format[item]) + ', '
            else:
                column_line = str(item) + ' ' + str(columns_format[item])
            
            # add columns statement to tables definitions
            tables_definitions += column_line
        
            # update counter
            n += 1

        # include columns definitions in create table statement
        create_table = create_table.format(table = table_name, tables_definitions = tables_definitions)

        # execute create table
        cursor.execute(create_table)

    ## INSERT VALUES
    ##########################################################################################################################################

    # insert statement
    insert_data = "INSERT INTO dbo.{table} VALUES {values_string}"

    # loop
    for line in tqdm.tqdm(tb.values.tolist()):

        # initializations
        values = []
        values_string = ""

        # create values list
        for i in range(0, ncolumns):
            values.append(format_value(line[i], columns_list[i], columns_format[columns_list[i]]))

        # create values string
        values_string = values_string + str(tuple(values))

        #print(values_string)
        # insert line
        cursor.execute(insert_data.format(table = table_name, values_string = values_string))
    
    ## END CONNECTION
    ##########################################################################################################################################

    # end connection and end cursor
    cnxn.commit()

    cursor.close()

    if (mode == 'overwrite'):
        return 'Table has been overwritten successfully!'
    else:
        return 'Table has been updated successfully!'
