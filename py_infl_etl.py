from datetime import datetime
import pandas as pd
#import matplotlib.pyplot as plt
from influxdb import InfluxDBClient

def run_py_infl_etl():
	query = 'SELECT * FROM "temperature" WHERE time >= now() - 1d ; '
	query1 = 'SELECT * FROM "temperature_bureau" WHERE time >= now() - 1d ; '

	
	client = InfluxDBClient('localhost',8086,database='test')

	result = client.query(query)
	result1 = client.query(query1)
	result2 = client.query(query2)
	result3 = client.query(query3)
	

	#print("Result: {0}".format(result))
	points = result.get_points()
	points1 = result1.get_points()
	points2 = result2.get_points()
	points3 = result3.get_points()
	



	dfs = pd.DataFrame(points)
	dfs['time'] = pd.to_datetime(dfs['time'], errors='coerce')
	dfs.set_index('time', inplace = True)
	dfs=dfs.resample('30s').mean()


	dfs1 = pd.DataFrame(points1)
	dfs1['time'] = pd.to_datetime(dfs1['time'])
	dfs1.set_index('time', inplace = True)
	dfs1=dfs1.resample('30s').mean()

	dfs2 = pd.DataFrame(points2)
	dfs2['time'] = pd.to_datetime(dfs2['time'], errors='coerce')
	dfs2.set_index('time', inplace = True)
	dfs2=dfs2.resample('30s').mean()


	dfs3 = pd.DataFrame(points3)
	dfs3['time'] = pd.to_datetime(dfs3['time'], errors='coerce')
	dfs3.set_index('time', inplace = True)
	dfs3=dfs3.resample('30s').mean()

	



	dfs=dfs.merge(dfs1, how='inner', on='time').merge(dfs2, how='inner', on='time').merge(dfs3, how='inner', on='time')
	dfs.set_axis(['Sensor(1)','Sensor(2)','Sensor(3)','Sensor(4)'], axis=1)
	dfs=dfs.fillna(method='ffill')	
	print(dfs)
	
	date = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")

	#ax=dfs['Sensor(1)'].plot()
	#ax=dfs['Sensor(2)'].plot()



	#plt.legend(loc='upper left')
	#ax.set_ylabel("Soil moisture")

	dfs.to_csv("csv/proto"+date+".csv")  
