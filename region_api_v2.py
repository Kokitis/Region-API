import configparser
import os
import pandas
import math
import sys
GITHUB_FOLDER = "D:\\Proginoskes\\Documents\\GitHub\\"
sys.path.append(GITHUB_FOLDER)
import pytools.tabletools as tabletools
import pytools.timetools as timetools
import pytools.numbertools as numbertools
import pytools.plottools as plottools
from databox import Databox
from pprint import pprint
from prettytable import PrettyTable

DATA_FOLDER = "D:\\Proginoskes\\Documents\\Data\\Harmonized Data\\"

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
		configurations = [
			{
				'name': 'World Economic Outlook',
				'filename': "data\\World Economic Outlook.tsv", #Filename
				'keyRegionCodeColumn': 'countryCode' #The column that defines the region codes.
			},
			{
				'name': "USA City Populations",
				'filename': os.path.join(DATA_FOLDER, "Country Tables\\United States\\1790-2010_MASTER.csv"),
				'keyRegionCodeColumn': 'cityStName'
			}
		]
		configurations = {i['name']:i for i in configurations}

		return configurations[name]
	def _catagorizeColumns(self, columns):
		""" Separates columns into timeseries or other 
		"""
		result = {'timeseries': [], 'dataseries': []}
		for column in columns:
			is_number_type = isinstance(column, (int, float))
			is_number_string = column.isdigit()
			is_number = is_number_type or is_number_string

			if is_number: result['timeseries'].append(column)
			else: result['dataseries'].append(column)

		return result

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
	def _parseCompactSeries(self, series):
		""" Transforms a series where columns are used as variable names,
			including time values.
			Parameters
			----------
				series: dict-like
		"""

		columns = self._catagorizeColumns(series.index)

		timeseries = list()
		other_data = list()
		for element in series.items():

			if element[0] in columns['timeseries']:
				timeseries.append(numbertools.toNumber(element))
			else:
				other_data.append(element)
		other_data = dict(other_data)
		#pprint(timeseries)

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
		series = self._parseCompactSeries(series)
		return series
	def _subjectList(self):
		""" Generates a table of all subjects contained in the table.
		"""

		subject_name_column = 'subjectName'
		subject_code_column = 'subjectCode'
		subject_description_column = 'subjectNotes'

		table = PrettyTable()
		table.field_names = [subject_code_column, subject_name_column, subject_description_column]
		seen = list()
		for _, row in self:
			if row[subject_code_column] in seen: continue
			else:
				seen.append(row[subject_code_column])
				subject_code = row[subject_code_column]
				
				if subject_name_column in row:
					subject_name = row[subject_name_column]
				else: subject_name = ""
				
				if subject_description_column in row:
					subject_text = row[subject_description_column]
				else: subject_text = ""
				if pandas.isnull(subject_text): subject_text = ""
				if len(subject_text) > 25: subject_text = subject_text[:25]
				
				table.add_row([subject_code, subject_name, subject_text])
		print(table)

	def _regionList(self):
		pass

	def _generalDescription(self):
		pprint(self.columns)


def test():
	dataset = Dataset('USA City Populations')
	databox = Databox()
	criteria = [('cityStName', 'Pittsburgh, PA')]
	left = dataset.request(criteria)
	right= dataset.request([('cityStName', 'Cleveland, OH')])
	left = left['timeseries']
	right= right['timeseries']
	comparison = databox.compare(left, right)
	#plot = plottools.PyplotXY()
	#plot.addSeries(series = comparison)
	#plot.addSeries(series = right)
	#plot.render()
	databox.generateTable(left)
	#dataset._subjectList()
if __name__ == "__main__" and True:
	test()
	"""
	timer = timetools.Timer()
	dataset = Dataset('World Economic Outlook')
	timer.timeit()
	

	series = dataset.request(criteria)
	#series = dataset(criteria)
	#pprint(series)
	pprint(dataset._subjectList())
	"""


