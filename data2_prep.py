'''
In this example we create 4 files containing 2 cumulative files and 2 deta files in between them.
The delta files contain new records as well as any record revisions (data2 contains a revision).
The second cumulative data file (data3) also contains a deleted record.
'''

import pandas as pd
pd.set_option('max_rows',500)


cols = ['key','tpep_dropoff_datetime','trip_distance','total_amount']

odf = pd.read_csv('/home/eric/DATA/yellow_tripdata_2018-12.csv')
odf = odf.reset_index().rename(columns={'index':'key'})


# define original data (first cumulative)
data0 = odf.query('key<=4')

# first incremental delta
data1 = odf.query('4>key<=6')

# second incremental delta
data2 = odf.query('5>key<=8') \
	.assign(total_amount = lambda x: (x.total_amount)*1.1)

# second cumulative file
data3 = pd.concat([data0, data1, data2, odf.query('8>key<=10')]) \
	.drop_duplicates(subset="key", keep='last') \
	.query('key !=4') # drop record #4 in this cumulative update

# save data
data0[cols].to_csv('data/cum_incr/data0.csv', index=False) # data0 has the starting point of a database (4 records)
data1[cols].to_csv('data/cum_incr/data1.csv', index=False) # data1 has first incremental delta (2 new records)
data2[cols].to_csv('data/cum_incr/data2.csv', index=False) # data2 has 1 revised record and 1 new record
data3[cols].to_csv('data/cum_incr/data3.csv', index=False) # data3 has all cumulative data, as well as new records, and deleted rows