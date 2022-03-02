# SnowPyContrib.py
#
# This is a class SnowPyContrib that has common Snowflake methods that may be commonly used in data science.
# The design is to store the connection variables in the class, create a connection, then run sql for higher level scripts.
#
# Copyright 2022 - Snowflake Inc.
#
# Code is for Example puproses, not officially part of the Snowflake Product.
# It is a class/lib that contains some internally developed as well as customer developed code.
# The intent is to help customers out short term while development and the snowpark groups work on formal UDTF and Matricies becomeing developed and released.
# ALL CODE IS Provided as is and is not under any warrenty nor support.
#
# Customer is free to alter, change, adapt for their own individual needs but Snowflake reserves the right to alter and distrubute updated copies 
# that may not be in sync with what a customer has changed.
#
# Requires Python3:
#   Install: Go to https://www.python.org/ and follow the directions for your OS/Cloud OS
# Requires Snowflake Connector for Python:
#   Upgrade pip3: pip3 install --upgrade setuptools pip
#           python3 -m pip install --upgrade pip
#   Install: python3 -V. (Cam back with 3.8.9)
#            pip3 install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.6.2/tested_requirements/requirements_38.reqs
#            -- This command will install the latest
#            pip3 install snowflake-connector-python
#            -- This command will install specifically 2.6.2 version
#            pip3 install snowflake-connector-python=2.6.2
#
#
# Written by: Ed Crean - Sr Solutions Architect, Financial Services Vertical.
#
#
# Usage Example:
#    another script would have the import SnowPyContrib line in it and then instantiate the class, and call methods.
#    import sys
#    sys.path.insert(0,'/Users/ecrean/Scripts/')
#    from SnowPyContrib import SnowPyContrib
#    Source = SnowPyContrib()
#    Source.SetConnVariables(REGION, ACCT, USER, PASS, ROLE, DW, DB, SCHEMA)
#    Source.GetConn()
#    Source.SQL = 'SELECT current_version();'
#    Source.Execute()
#
#
#------------------------------------------------------------------------------------
import os 
import sys
import time
import datetime
from   datetime import date
import argparse
import functools
import string
import snowflake.connector
import pandas as pd



###########################################################################################
class SnowPyContrib:
   # Init will be invoked on instantiation
   # We will use it to declare connection based variables as well as temp storage for execution and loops
   def __init__(self):
      # Connection Variables - Initialize to all NULL
      self.Region = ''
      self.Acct = ''
      self.User = ''
      self.Pass = ''
      self.Role = ''
      self.DW = ''
      self.DB = ''
      self.Schema = ''
      # Data and Connection and Cursor
      self.RawData = []
      self.conn = None
      self.curr = None
      # SQL to be executed
      self.SQL = 'INVALID SQL;' # To protect from someone calling execute without setting this variable to executable code.  We want to gen an error in that situation.
      # Possible Data Structures that may be useful in generating SQL code
      self.TableList = []
      self.KeyList = []
      self.DataList = []
      self.ColumnList = []


   # Methods
   #--------------------
   # Connection Related
   # Close a Connection
   def Close(self):
      try:
         print("Closing Connection " + type(self).__name__ + ":")
         self.conn.close()
         print("Connection Closed Successfully")

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Connection Close Failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method Close:")
      except Exception as e:
         print("General Exception in method Close:")
         print(e)
         raise Exception("Failure in SnowPyContrib method Close:")


   # Read in Connection JSON File and populate Connection Variables
   def SetConnVariablesFromJSON(self, FileName, FileLocationType):
      # Files could be Local, S3, Vault storeed...
      tbd = """
      try:
         print("Open and read the JSON File with the connection info")
         FullPath = JsonDirName + JsonFileName
         if not os.path.exists(FullPath):
            print("JSON File with Source Connection Information not found...")
            sys.exit(1)
         with open(FullPath, 'r') as JSONOpen:
            JSONSrcObj = json.load(JSONOpen)

      except Exception as e:
         print("Failed to Open the JSON file with connection info or failed to read the data from it...")
         print(e)
         sys.exit(1)
      """

      # Setup the variables needed for a connection
      try:
         self.Region = Region
         self.Acct = Acct
         self.User = User
         self.Pass = Pass
         self.Role = Role
         self.DW = DW
         self.DB = DB
         self.Schema = Schema

      except snowflake.connector.errors.ProgrammingError as e:
         print("Setting Connection Variables in self Failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method SetConnVariablesFromJSON:")
      except Exception as e:
         print("General Exception setting Connection Variables in self:")
         print(e)
         raise Exception("Failure in SnowPyContrib method SetConnVariablesFromJSON:")


   # Populate Connection Variables
   def SetConnVariables(self, Region, Acct, User, Pass, Role, DW, DB, Schema):
      # Setup the variables needed for a connection
      try:
         self.Region = Region
         self.Acct = Acct
         self.User = User
         self.Pass = Pass
         self.Role = Role
         self.DW = DW
         self.DB = DB
         self.Schema = Schema

      except snowflake.connector.errors.ProgrammingError as e:
         print("Setting Connection Variables in self Failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method SetConnVariables:")
      except Exception as e:
         print("General Exception setting Connection Variables in self:")
         print(e)
         raise Exception("Failure in SnowPyContrib method SetConnVariables:")


   # Open a Standard Connection
   def GetConn(self):
      try:
         # Connect to the DW and execute a simple test query
         print("Attempt to Open Connection: " + self.Region + ":" + self.Acct + ":" + self.User + ":" + self.DW + ":" + self.DB )
         self.conn = snowflake.connector.connect(
             region=self.Region,
             account=self.Acct,
             user=self.User,
             password=self.Pass,
             warehouse=self.DW,
             database=self.DB,
             schema=self.Schema
             )

         print("Got the Connection")
         print("Get a Cursor")
         self.curr = self.conn.cursor()
         print("Got a Cursor")

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Connection Create Failed or Cursore Create Failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method GetConn:")
      except Exception as e:
         print("General Exception when creating connection or cursor:")
         print(e)
         raise Exception("Failure in SnowPyContrib method GetConn:")


   # Open a Browser Based Connection
   def GetBrowserConn(self):
      try:
         # Connect to the DW and execute a simple test query
         print("You MUST have an active Browser Connection Open to Snowflake Workbooks for this to work")
         print("Open Connection: " + self.Region + ":" + self.Acct + ":" + Self.User )
         self.conn = snowflake.connector.connect(
             region=self.Region,
             user=self.User,
             authenticator='externalbrowser',
             account=self.Acct,
             )

         print("Got the Connection")
         print("Get a Cursor")
         self.curr = self.conn.cursor()
         print("Got a Cursor")

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Connection Close Failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method GetBrowserConn:")
      except Exception as e:
         print("General Exception in method Close:")
         print(e)
         raise Exception("Failure in SnowPyContrib method GetBrowserConn:")


   # Change Warehouse
   def ChangeDW(self, Warehouse):
      try:
         # Change to use a different Data Warehouse and use a Local SQLString
         print("Changing Warehouse to: " + Warehouse)
         SQLString = "USE WAREHOUSE " + Warehouse + " ;"
         self.curr.execute(SQLString)  

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Use Warehouse Change Failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method ChangeDW:")
      except Exception as e:
         print("General Exception in Use Warehouse Change failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method ChangeDW:")


   # Change Database
   def ChangeDB(self, Database):
      try:
         # Change to use a different Database and use a Local SQLString
         print("Changing Database to: " + Database)
         SQLString = "USE DATABASE " + Database + " ;"
         self.curr.execute(SQLString)  

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Use Warehouse Change Failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method ChangeDB:")
      except Exception as e:
         print("General Exception in Use Warehouse Change failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method ChangeDB:")


   # Change Schema
   def ChangeSchema(self, Schema):
      try:
         # Change to use a different Schema and use a Local SQLString
         print("Changing Schema to: " + Schema)
         SQLString = "USE SCHEMA " + Schema + " ;"
         self.curr.execute(SQLString)  

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Use Schema Change Failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method ChangeSchema:")
      except Exception as e:
         print("General Exception in Use Schema Change failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method ChangeSchema:")


   # Change Role
   def ChangeRole(self, Role):
      try:
         # Change to use a different Role and use a Local SQLString
         print("Changing Role to: " + Role)
         SQLString = "USE ROLE " + Role + " ;"
         self.curr.execute(SQLString)  

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Use Role Change Failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method ChangeRole:")
      except Exception as e:
         print("General Exception in Use Role Change failed:")
         print(e)
         raise Exception("Failure in SnowPyContrib method ChangeRole:")


   # EXECUTION METHODS
   # Set the SQL string within the class
   # They could also directly set the SQL in the Object at a higher level as well.
   def SetSQL(self, SQLString):
      try:
         self.SQL = SQLString

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake error when setting the self.SQL string within the class:")
         print(e)
         raise Exception("Failure in SnowPyContrib method SetSQL:")
      except Exception as e:
         print("General Exception when setting the self.SQL string within the class:")
         print(e)
         raise Exception("Failure in SnowPyContrib method SetSQL:")


   # Execute SQL Stored within the class
   def Execute(self):
      try:
         # Execute SQL String set in class
         print("Execute: " + self.SQL)
         self.curr.execute(self.SQL)  

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method Execute:")
      except Exception as e:
         print("General Exception when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method Execute:")


   # Execute SQL Stored within the class
   def ExecuteSilent(self):
      try:
         # Execute SQL String set in class
         self.curr.execute(self.SQL)  

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method Execute:")
      except Exception as e:
         print("General Exception when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method Execute:")


   # Execute SQL String passed to the method
   def ExecuteSQL(self, SQLString):
      try:
         # Execute SQL String set in class
         print("Execute: " + SQLString)
         self.curr.execute(SQLString)  

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + SQLString)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteSQL:")
      except Exception as e:
         print("General Exception when Executing SQL: " + SQLString)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteSQL:")


   # Execute SQL Stored within the class and display results
   def ExecuteDisplay(self):
      try:
         # Execute SQL String set in class
         print("Execute: " + self.SQL)
         self.curr.execute(self.SQL)

         RawResults = self.curr.fetchall()
         for row in RawResults:
             print(row)

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteDisplay:")
      except Exception as e:
         print("General Exception when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteDisplay:")


   # Execute SQL Stored within the class and display results
   def ExecuteDisplaySilent(self):
      try:
         # Execute SQL String set in class
         self.curr.execute(self.SQL)

         RawResults = self.curr.fetchall()
         for row in RawResults:
             print(row)

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteDisplay:")
      except Exception as e:
         print("General Exception when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteDisplay:")


   # Execute SQL String passed to the method and display results
   def ExecuteSQLDisplay(self, SQLString):
      try:
         # Execute SQL String set in class
         print("Execute: " + SQLString)
         self.curr.execute(SQLString)  

         RawResults = self.curr.fetchall()
         for row in RawResults:
             print(row)

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + SQLString)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteSQLDisplay:")
      except Exception as e:
         print("General Exception when Executing SQL: " + SQLString)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteSQLDisplay:")


   # Execute SQL String passed that is SELECT COUNT(*) style and return the row count returned
   def ExecuteRetRowCount(self):
      try:
         # Execute SQL String set in class
         print("Execute: " + self.SQL)
         self.curr.execute(self.SQL)

         RawResults = self.curr.fetchall()
         # Looping, but should only be one result row
         for row in RawResults:
             # The row should only be one column
             OutString = str(row[0])
             print(OutString)

         return OutString

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteRetRowCount:")
      except Exception as e:
         print("General Exception when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteRetRowCount:")


   def ExecuteGetTableCount(self, TableName):
      try:
         # Execute SQL String set in class
         SQLString = "SELECT COUNT(*) FROM " + TableName + ";"
         self.SQL = SQLString
         print("Execute: " + self.SQL)
         self.curr.execute(self.SQL)

         RawResults = self.curr.fetchall()
         # Looping, but should only be one result row
         for row in RawResults:
             # The row should only be one column
             OutString = str(row[0])
             print(OutString)

         return OutString

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteRetRowCount:")
      except Exception as e:
         print("General Exception when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteRetRowCount:")


   # Execute SQL String passed to the method and display results
   def ExecuteRetRawResults(self):
      try:
         # Execute SQL String set in class
         print("Execute: " + self.SQL)
         self.curr.execute(self.SQL)
         RawResults = self.curr.fetchall()
         for row in RawResults:
             print(row)

         return RawResults

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteRetRawResults:")
      except Exception as e:
         print("General Exception when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteRetRawResults:")


   # Execute SQL String passed to the method and display results
   def ExecuteDisplayDescriptionTest(self):
      try:
         # Execute SQL String set in class
         print("Describe SQL before running it")
         result_metadata_list = self.curr.describe(self.SQL)
         print(','.join([col.name for col in result_metadata_list]))

         print("Execute: " + self.SQL)
         self.curr.execute(self.SQL)

         print("Snowflake QueryID:")
         print(self.curr.sfqid)

         print(','.join([col[0] for col in self.curr.description]))

         RawResults = self.curr.fetchall()
         for row in RawResults:
             print(row)

         return RawResults

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteRetRawResults:")
      except Exception as e:
         print("General Exception when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method ExecuteRetRawResults:")



   # <<<  ADD EXECUTE AND RETURN STRING >>>




   # <<<  ADD EXECUTE AND RETURN LIST >>>



   # <<<  ONE HOT ENCODING  >>>
   # Pass an input table and an ouput table name
   # Function will create a new output table based on the input table
   # All string/varchar data type columns will be inflated to individual columns based on the number of values for the string
   # there is a limit of 100 on the number of string values.
   def SciOneHotEncode_TF(self, InputTable, OutputTable):
      try:
         # Lets get the datatype information for each column
         SQLString = "SELECT a.ORDINAL_POSITION, a.COLUMN_NAME, a.DATA_TYPE FROM "
         SQLString += self.DB + ".information_schema.columns a WHERE TABLE_SCHEMA = \'" + self.Schema + "\' AND TABLE_NAME = \'" + InputTable + "\' ORDER BY a.ORDINAL_POSITION;"   
         self.SQL = SQLString
         RawResults = self.ExecuteRetRawResults();
         print("Debug: select from information schema worked")

         # Now lets get through the Columns returned and build the next set of SQL
         # For anything a String, we will expand.
         # For anything a Bool, we will code True to 1 and everything else to 0
         # For Variants, we will skip.
         TableCreateSQL = "CREATE OR REPLACE TRANSIENT TABLE " + self.Schema + "." + OutputTable + " AS SELECT "
         print("Debug: start loop")
         for row in RawResults:
            print("In Loop")
            OutString = str(row)
            print(row)
            print("Debug: set ColName")
            ColName = row[1]
            print("Debug: set DataType")
            DataType = row[2]
            print("Debug: Entering if logic")
            if (DataType == "VARIANT"):  
               continue 
            elif (DataType == "BOOLEAN"):
               TableCreateSQL += "CASE WHEN " + ColName + " = TRUE THEN 1 WHEN " + ColName + " = FALSE THEN 0 ELSE 0 END AS " + ColName 
               TableCreateSQL += ", " 
            elif (DataType == "TEXT"): 
               ColCountSQL = "SELECT COUNT(*) FROM (SELECT DISTINCT " + ColName + " FROM " + self.Schema + "." + InputTable + ");"
               self.SQL = ColCountSQL
               CountStr = self.ExecuteRetRowCount()
               Count = int(CountStr)
               if (Count >= 101):
                   raise Exception("SciOneHotEncode_TF encountered string with over 100 values in it... FAILING and EXIITING!!!")
               ColSQL = "SELECT DISTINCT " + ColName + " FROM " + self.Schema + "." + InputTable + ";"
               self.SQL = ColSQL
               ColRawResults = self.ExecuteRetRawResults()
               print("Debug: Start Column based loop")
               for ColString in ColRawResults:
                   ColValueName = ColString[0]
                   TableCreateSQL += "CASE WHEN " + ColName + " = \'" + ColValueName + "\' THEN 1 ELSE 0 END AS " + ColName + "_" + ColValueName 
                   TableCreateSQL += ", " 
               # Remove last column now
               print("Debug: Loop Done: " + TableCreateSQL)           
            else:             
               TableCreateSQL += ColName + ", "
            
        
         # End of loop
         # Remove the last 2 characters from the string to get rid of the last comma
         StringSize = len(TableCreateSQL)
         TempStr = TableCreateSQL[:StringSize - 2]
         TableCreateSQL = TempStr
         # Tack on the FROM part of the SQL statement
         TableCreateSQL += " FROM " + self.Schema + "." + InputTable + ";"

         print("Debug: Execute table create SQL that was constructed...")
         # Set and Execute it
         self.SQL = TableCreateSQL
         self.Execute()
         print("Debug: Execute Finished...")

         return OutputTable

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method SciOneHotEncode_TF:")
      except Exception as e:
         print("General Exception when processing SQL results or generating SQL statements: ")
         print(e)
         raise Exception("Failure in SnowPyContrib method SciOneHotEncode_TF:")



   # <<<  CORRELATION Passed Y and List of Xs  >>>
   def SciCorrTablePassY_PassXs(self, InputTable, YCol, XString):
      try:
         FirstRow = True
         SQLString = "SELECT "
         # Lets break that string of x columns into a list to work with 
         XList = XString.split(',')
         # Now loop on each element
         for RawCol in XList:
             # Get rid of white space anywhere
             XCol = RawCol.strip()
             if FirstRow:
                FirstRow = False
             else:
                SQLString += ","
                SQLString += "\n"
             # Now add the SQL Code for the next X column to be processed
             SQLString += "(sum(" + YCol + "*" +  XCol + ") - (sum(" + XCol + ")*sum("+ YCol + ")/(count(*))))/"
             SQLString += "((SQRT(sum(" + XCol + "*" + XCol + ")-(sum(" + XCol + ")*sum(" + XCol + "))/(count(*))))*"
             SQLString += "(SQRT(sum(" + YCol + "*" + YCol + ")-(sum(" + YCol + ")*sum(" + YCol + "))/(count(*))))) AS COR_2Y_" + XCol
         # End For Loop
         SQLString += " FROM " + InputTable + ";"
         self.SQL = SQLString
         print("----------------------------------------")
         print(XString)
         self.ExecuteDisplaySilent()

         return

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method SciOneHotEncode_TF:")
      except Exception as e:
         print("General Exception when processing SQL results or generating SQL statements: ")
         print(e)
         raise Exception("Failure in SnowPyContrib method SciOneHotEncode_TF:")



   # <<<  CORRELATION Passed Y and ALL Xs  >>>
   def SciCorrTablePassY_AllXs(self, InputTable, YCol):
      try:
         # Need to build a list of X Columns from the table.
         # We will not be filtering on data type, but that could be added
         # For now, we assume that the table has been prepped to have all numerics in it
         #
         # Lets get the datatype information for each column
         Temp = YCol.upper()
         YCol = Temp
         SQLString = "SELECT a.ORDINAL_POSITION, a.COLUMN_NAME, a.DATA_TYPE FROM "
         SQLString += self.DB + ".information_schema.columns a WHERE TABLE_SCHEMA = \'" + self.Schema + "\' AND TABLE_NAME = \'" + InputTable + "\' ORDER BY a.ORDINAL_POSITION;"   
         self.SQL = SQLString
         RawResults = self.ExecuteRetRawResults();
  
         # We are going to take all the columns and put them in a comma separated string for use in the loop further down where we gen the SQL.
         XColList = []
         XColString = ''
         for RawCol in RawResults:
             TempStr = RawCol[1]
             XCol = TempStr.strip()
             if (XCol == YCol):
                continue
             XColList.append(XCol)
             XColString += XCol + ",        "
         # End of loop building column list

         SQLString = "SELECT "
         FirstRow = True
         for XCol in XColList:
             if FirstRow:
                FirstRow = False
             else:
                SQLString += ","
                SQLString += "\n"
             # Now add the SQL Code for the next X column to be processed
             SQLString += "(sum(" + YCol + "*" +  XCol + ") - (sum(" + XCol + ")*sum("+ YCol + ")/(count(*))))/"
             SQLString += "((SQRT(sum(" + XCol + "*" + XCol + ")-(sum(" + XCol + ")*sum(" + XCol + "))/(count(*))))*"
             SQLString += "(SQRT(sum(" + YCol + "*" + YCol + ")-(sum(" + YCol + ")*sum(" + YCol + "))/(count(*))))) AS COR_2Y_" + XCol
         # End For Loop
         SQLString += " FROM " + InputTable + ";"
         self.SQL = SQLString
         print("----------------------------------------")
         print(XColString)
         self.ExecuteDisplaySilent()

         return

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method SciOneHotEncode_TF:")
      except Exception as e:
         print("General Exception when processing SQL results or generating SQL statements: ")
         print(e)
         raise Exception("Failure in SnowPyContrib method SciOneHotEncode_TF:")


   # <<<  CORRELATION of everything in the table  >>>
   # SciCorrTableAllXs
   def SciCorrTableAllXs(self, InputTable):
      try:
         # Need to build a list of X Columns from the table.
         # We will not be filtering on data type, but that could be added
         # For now, we assume that the table has been prepped to have all numerics in it
         #
         # Lets get the datatype information for each column
         SQLString = "SELECT a.ORDINAL_POSITION, a.COLUMN_NAME, a.DATA_TYPE FROM "
         SQLString += self.DB + ".information_schema.columns a WHERE TABLE_SCHEMA = \'" + self.Schema + "\' AND TABLE_NAME = \'" + InputTable + "\' ORDER BY a.ORDINAL_POSITION;"   
         self.SQL = SQLString
         RawResults = self.ExecuteRetRawResults();
  
         # We are going to take all the columns and put them in a comma separated string for use in the loop further down where we gen the SQL.
         XColList = []
         HeaderStr = "        "
         for RawCol in RawResults:
             TempStr = RawCol[1]
             XCol = TempStr.strip()
             XColList.append(XCol)
             HeaderStr += XCol + ",          "
         # End of loop building column list

         # Need to build a column list across the TOP and down the side...
         print("----------------------------------------")
         print(HeaderStr)
         for YCol in XColList:
            SQLString = "SELECT '" + YCol + " - ' || "
            FirstRow = True
            for XCol in XColList:
                if FirstRow:
                   FirstRow = False
                else:
                   SQLString += ","
                   SQLString += "\n"
                # Now add the SQL Code for the next X column to be processed
                SQLString += "(sum(" + YCol + "*" +  XCol + ") - (sum(" + XCol + ")*sum("+ YCol + ")/(count(*))))/"
                SQLString += "((SQRT(sum(" + XCol + "*" + XCol + ")-(sum(" + XCol + ")*sum(" + XCol + "))/(count(*))))*"
                SQLString += "(SQRT(sum(" + YCol + "*" + YCol + ")-(sum(" + YCol + ")*sum(" + YCol + "))/(count(*))))) AS COR_" + XCol
            # End For Loop
            SQLString += " FROM " + InputTable + ";"
            self.SQL = SQLString
            self.ExecuteDisplaySilent()
         # End of loop

         return

      except snowflake.connector.errors.ProgrammingError as e:
         print("Snowflake Threw Error when Executing SQL: " + self.SQL)
         print(e)
         raise Exception("Failure in SnowPyContrib method SciOneHotEncode_TF:")
      except Exception as e:
         print("General Exception when processing SQL results or generating SQL statements: ")
         print(e)
         raise Exception("Failure in SnowPyContrib method SciOneHotEncode_TF:")






