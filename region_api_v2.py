import configparser
import os
import pandas

import sys
sys.path.append("D:\\Proginoskes\\Documents\\GitHub\\pytools")
import tabletools
import timetools
from pprint import pprint

class Dataset(tabletools.Table):
	"""
		Output Format
		-------------
			{
				*identifiers
				*'timeseries': list()
				*'data': dict<>
			}
	"""
	def __init__(self, name, **kwargs):
		self.configuration = self._getDatasetConfiguration(name)

		filename = self.configuration['filename']
		kwargs = {"sheetname": 0}
		super().__init__(filename, **kwargs)

	def _getDatasetConfiguration(self, name):
		"""
			Configuration
			-------------
			{
				'filename': string
					Path to the dataset
				''
			}
		"""
		configurations = {
			'World Economic Outlook':
			{
				'filename': "data\\World Economic Outlook.tsv", #Filename
				'keyRegionCodeColumn': 'countryCode' #The column that defines the region codes.
			}
		}

		return configurations[name]
	def _getSeparatedColumns(self, columns):
		timecols = list()
		othercols = list()
		for col in columns:
			isdigit  =False
			if isinstance(col, (int, float)):
				isdigit = True
			elif not any(not i.isdigit() for i in col):
				isdigit = True

			if isdigit:
				timecols.append(col)
			else: othercols.append(col)

		return timecols, othercols

	def _getIdentifierFields(self, series):
		""" Extracts region identifiers from the series. Includes
			region names and region codes.
		"""
		identifiers = dict()
		if 'regionCode' in series:
			identifiers['regionCode'] = series['regionCode']
		if 'regionName' in series:
			identifiers['regionName'] = series['regionName']
		if 'countryCode' in series:
			identifiers['countryCode'] = series['countryCode']
		if 'countryName' in series:
			identifiers['countryName'] = series['countryName']

		return identifiers
	def _parseTableSeries(self, series):
		""" Transforms a series where columns are used as variable names,
			including time values.
			Parameters
			----------
				series: dict-like
		"""

		time_columns, other_columns = self._getSeparatedColumns(series.index)

		timeseries = list()
		other_data = list()
		for element in series.items():

			if element[0] in time_columns:
				timeseries.append(element)
			else:
				other_data.append(element)
		other_data = dict(other_data)
		other_data['timeRange'] = [min(timeseries, key = lambda s:s[0])[0],
								   max(timeseries, key = lambda s:s[0])[0]]
		other_data['dataRange'] = [min(timeseries, key = lambda s:s[1])[1],
								   max(timeseries, key = lambda s:s[1])[1]]		


		identifiers = self._getIdentifierFields(series)
		response = {
			'data': other_data,
			'availableFields': list(other_data.keys()),
			'timeseries': timeseries
		}
		if len(identifiers) != 0:
			response.update(identifiers)
		return response
	def request(self, criteria):
		"""

		"""
		series = self(criteria)
		series = self._parseTableSeries(series)
		return series



if __name__ == "__main__" and True:
	timer = timetools.Timer()
	dataset = Dataset('World Economic Outlook')
	timer.timeit()
	criteria = [('countryCode', 'USA'), ('subjectCode', 'NGDPD')]

	series = dataset.request(criteria)
	#series = series.iloc[0]
	pprint(series)


