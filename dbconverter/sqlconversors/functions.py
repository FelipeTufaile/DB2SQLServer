##########################################################################################################################################
## EXCEL TO SQL SERVER
##########################################################################################################################################

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



##########################################################################################################################################
## POSTGRESQL TO SQL SERVER
##########################################################################################################################################

## postgresql2sqlserver converter funtion:
def postgresql2sqlserver(table_path, credentials):

    """
    Input:
    1 - table_path: path of the table that has to be loaded into a database    | string
    2 - credentials: credentials information for given access to the database  | dictionary

    Output:
    1 - Succes/ Failure notification.
    """

    #import libraries
    import pyodbc
    import pandas as pd
    import re
    import tqdm
    import math
    import pgdumplib
    from time import sleep

    ## FUNCTIONS DEFINITIONS
    ##########################################################################################################################################

    def execute_command(connection_string, command_string, read_columns = False):
        
        # start connection using pyodbc
        cnxn = pyodbc.connect(connection_string)
        
        # create cursor
        cursor = cnxn.cursor()

        if(read_columns):

            # read columns
            columns = [column[0] for column in cursor.execute(command_string).description]

            # end connection
            cnxn.commit()

            # end cursor
            cursor.close()

            return columns
        
        else:

            # execute command
            cursor.execute(command_string)

            # end connection
            cnxn.commit()

            # end cursor
            cursor.close()


    ## SQL STATEMENTS
    ##########################################################################################################################################

    # insert statement
    insert_data = "INSERT INTO dbo.{table} VALUES {values_string}"

    # DROP TABLE IF EXISTS
    drop_table = "IF OBJECT_ID (N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}"

    # READ COLUMNS
    read_columns = "SELECT * FROM dbo.{table}"

    ## GET TABLE'S IFORMATION
    ##########################################################################################################################################
    # read table
    print("Reading file: " + str(list(table_path.split('\\'))[-1]))
    dump = pgdumplib.load(table_path)

    tables = {}
    for element in dump.entries:
        if element.desc == 'TABLE':
            tables[element.tag] = element.namespace

    ## START CONNECTION
    ##########################################################################################################################################

    # read credentials
    driver = '{ODBC Driver 17 for SQL Server}'
    server = credentials['server']
    database = credentials['database']
    username = credentials['username']
    password = credentials['password']

    # create connection string frame
    connection_string_frame = 'Driver={driver};Server=tcp:{server},1433;Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    # create connection string
    connection_string = connection_string_frame.format(driver = driver, server = server, database = database, username = username, password = password)

    ## INITIALIZE TABLE
    ##########################################################################################################################################

    # there are two types of mode 'overwrite' and 'append'. Append is the default and it appends the information (new entries) on an existing
    # table. Overwrite is an option that has to be set in 'mode' argument when calling 'excel2sqlserver' function. It overwrites an existing table
    # if it already exists or it creates a new table with a given name (also given as a function argument).

    for table in tables:

        print("Updating table: " +str(table))


        # EXECUTE DROP TABLE
        execute_command(connection_string = connection_string, command_string = drop_table.format(table = table), read_columns = False)

        # CREATE TABLE
        create_table = dump.lookup_entry('TABLE', tables[table], table).defn

        # ADJUSTMENTS
        ###################################################################################################################
        ## remove schema name
        create_table = create_table.replace(tables[table] + '.', '')

        ## change timestamp type to string: incosistency between postgresql and sqlserver
        create_table = create_table.replace('timestamp without time zone', 'character varying(100)')

        ## change boolean type to string: incosistency between postgresql and sqlserver
        create_table = create_table.replace('boolean,', 'character varying(100),')
        create_table = create_table.replace('boolean\n', 'character varying(100)\n')
        create_table = create_table.replace('boolean NOT NULL', 'character varying(100) NOT NULL')
        create_table = create_table.replace('boolean DEFAULT false', 'character varying(100) NOT NULL')
        create_table = create_table.replace('DEFAULT now() NOT NULL', 'NOT NULL')

        # EXECUTE CREATE TABLE
        execute_command(connection_string = connection_string, command_string = create_table, read_columns = False)

        # UPDATE TABLE
        ###################################################################################################################
        # read columns 
        columns = execute_command(connection_string = connection_string, command_string = read_columns.format(table = table), read_columns = True)

        # read all rows of choosen table
        lines = []
        for row in dump.table_data(tables[table], table):
            if(len(row) < len(columns)):
                lrow = list(row)
                lrow.append('')
                row = tuple(lrow)
            lines.append(row)

        # create pandas dataframe
        tb = pd.DataFrame(lines, columns = columns)

        # check the appropriate size for each column
        for colname in columns:
            pattern = colname + ' character varying'
            for statement in create_table.split('\n'):
                if(pattern in statement):
                    number_before = re.findall(r'\d+', statement)
                    try:
                        number_after = math.ceil(tb.loc[tb[colname].isnull()==False][colname].str.len().max()/100)*100
                    except ValueError:
                        number_after = 200
                    if(number_before == []):
                        sentence_before = pattern
                        sentence_after  = pattern + '({number})'.format(number = 100)
                    else:
                        sentence_before = pattern + '({number})'.format(number = number_before[0])
                        sentence_after  = pattern + '({number})'.format(number = max(number_after, 100))

                    create_table = create_table.replace(sentence_before, sentence_after)

        # DROP TABLE AND RESIZE COLUMNS
        ###################################################################################################################

        # EXECUTE DROP TABLE
        execute_command(connection_string = connection_string, command_string = drop_table.format(table = table), read_columns = False)

        # EXECUTE CREATE TABLE
        execute_command(connection_string = connection_string, command_string = create_table, read_columns = False)

        ## INSERT VALUES
        ##########################################################################################################################################

        # initializations
        counter, first_element, values_string = 1, True, ""

        # loop
        for line in tqdm.tqdm(tb.values.tolist()):

            # initializations
            values = []

            # create values list
            for value in line:
                if(value == None):
                    values.append('')
                else:
                    value_updated = value
                    value_updated = value_updated.replace("'", "")
                    value_updated = value_updated.replace('"', '')
                    value_updated = value_updated.replace("None", "Null")
                    value_updated = value_updated.replace("\\r", "")
                    value_updated = value_updated.replace('\\n', '')
                    value_updated = value_updated.replace('\\', '')
                    values.append(value_updated)

            # create values string
            if(first_element == True):
                values_string = values_string + str(tuple(values))
                first_element = False
            else:
                values_string =  values_string + "," + str(tuple(values))

            # insert line
            if(counter == 1000):
                # insert lines
                execute_command(connection_string = connection_string, command_string = insert_data.format(table = table, values_string = values_string), read_columns = False)

                # reset parameters
                counter, first_element, values_string = 0, True, ""

            # update counter
            counter += 1

        # last insertion
        if(counter > 1):
            # insert lines
            execute_command(connection_string = connection_string, command_string = insert_data.format(table = table, values_string = values_string), read_columns = False)
  

