'''
Created on Nov 7, 2015
This program obtains data on the S&P 500 companies and stores them 
in an SQL database
@author: djunh
'''

#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
#import lxml.html
import pymysql as mdb
import finsymbols

from math import ceil


def obtain_parse_snp500():
    """Obtain and parse all symbols from S&P 500.  

     Returns a list of tuples for to add to MySQL."""

    # Stores the current time, for the created_at record
    now = datetime.datetime.utcnow()

    # Use finsymbols to download stock symbols, industry and S&P 500 data
    sp500=finsymbols.get_sp500_symbols()
    # Obtain the symbol information for each row in the S&P500 constituent table
    symbols = []
    for index, symbol in enumerate(sp500):
        sd = {'ticker': sp500[index]['symbol'],
         'name': sp500[index]['company'],
         'sector': sp500[index]['industry']}
        # Create a tuple (for the DB format) and append to the grand list
        symbols.append( (sd['ticker'], 'stock', sd['name'], 
                         sd['sector'], 'USD', now, now) )
      
    return symbols


def insert_snp500_symbols(symbols):
    """Insert S&P500 symbols into MySQL database."""

    # Connect to the MySQL instance
    db_host = 'localhost'
    db_user = 'sec_user'
    db_pass = 'password'
    db_name = 'securities_master'
    con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

    # Create the insert strings
    column_str = "ticker, instrument, name, sector, currency, created_date, last_updated_date"
    insert_str = ("%s, " * 7)[:-2]
    final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
    print final_str, len(symbols)

    # Using the MySQL connection, carry out an INSERT INTO for every symbol
    with con: 
        cur = con.cursor()
        # This line avoids the MySQL MAX_PACKET_SIZE
        # Although of course it could be set larger!
        for i in range(0, int(ceil(len(symbols) / 100.0))):
            cur.executemany(final_str, symbols[i*100:(i+1)*100-1])

if __name__ == "__main__":
  symbols = obtain_parse_snp500()
  #insert_snp500_symbols(symbols)