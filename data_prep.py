import pandas as pd
pd.set_option('max_rows',500)


num_samples = 8
cols = ['key','tpep_dropoff_datetime','trip_distance','total_amount']

df = pd.read_csv('/home/eric/DATA/yellow_tripdata_2018-12.csv')
df = df.reset_index().rename(columns={'index':'key'})

# define original data
temp0 = df.sample(num_samples)

# define data table
temp1 = temp0\
	.sample(int(num_samples/2))\
	.assign(total_amount = lambda x: (x.total_amount)*1.1)

# define delta+new_records+removed_records table
temp2 = pd.concat([
	temp1.sample(int(num_samples/4))\
		.assign(total_amount = lambda x: (x.total_amount)*1.1),
	df.sample(int(num_samples/4))])

# save data
temp0[cols].to_csv('data/data0.csv', index=False)
temp1[cols].to_csv('data/data1.csv', index=False)
temp2[cols].to_csv('data/data2.csv', index=False)