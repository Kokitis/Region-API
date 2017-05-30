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
				'name': 'World Development Indicators',
				'filename': os.path.join(DATA_FOLDER,
					"Global Tables", "World Development Indicators.txt"),
				'keyRegionCodeColumn': 'countryCode'

			},
			{
				'name': 'World Economic Outlook',
				'filename': os.path.join(DATA_FOLDER, "Global Tables", "World Economic Outlook.xlsx"), #Filename
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
			is_number = numbertools.isNumber(column) or column.isdigit()

			if is_number: result['timeseries'].append(column)
			else: result['dataseries'].append(column)

		return result
	def _getScaleMultiplier(self, value):
		if isinstance(value, str):
			string = value.lower()
			if string == 'trillions':
				multiplier = 1E12
			elif string == 'billions':
				multiplier = 1E9
			elif string == 'millions':
				multiplier = 1000000
			elif string == 'thousands':
				multiplier = 1000
			else:
				multiplier = 1
		else:
			multiplier = value
		return multiplier
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
		timeseries = list()
		other_data = list()
		if series is None:
			columns = list()
			identifiers = list()
			series = dict()
			response_status = False
		else:
			response_status = True
			columns = self._catagorizeColumns(series.index)
			identifiers = self._getIdentifierFields(series)


		for column, value in series.items():
			if column in columns['timeseries']:
				value = numbertools.toNumber(value)
				column= numbertools.toNumber(column)
				if 'scale' in series:
					multiplier = self._getScaleMultiplier(series['scale'])
					value *= multiplier
				timeseries.append((column, value))
			else:
				other_data.append((column, value))
		other_data = dict(other_data)

		if len(timeseries) > 0:
			other_data['timeRange'] = [min(timeseries, key = lambda s:s[0])[0],
									   max(timeseries, key = lambda s:s[0])[0]]
			other_data['dataRange'] = [min(timeseries, key = lambda s:s[1])[1],
									   max(timeseries, key = lambda s:s[1])[1]]
		else:
			other_data['timeRange'] = []
			other_data['datarange'] = []

		response = {
			'data': other_data,
			'availableFields': list(other_data.keys()),
			'response': response_status,
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
class Datasets:
	""" Manages several separate datasets.
	"""
	def __init__(self, datasets):
		""" 
			Parameters
			----------
				datasets: list<string>
					list of datasets to load.
		"""
		self.datasets = [Dataset(i) for i in datasets]
	def __call__(self, criteria):
		result = [dataset(criteria) for dataset in self.datasets]
		return result
class GeoChart:
	""" Easily plots and compares different series.
	"""
class ComparisonTable:
	def __init__(self, left, right):
		table = PrettyTable(field_names = ['Year', 'Left', 'Right'])
		left_timeseries = left['timeseries']
		right_timeseries= right['timeseries']
		left_timeseries = dict(left_timeseries)
		right_timeseries= dict(right_timeseries)
		
		#help(table)
		for year in left_timeseries.keys():
			table.add_row([year, left_timeseries[year], right_timeseries.get(year)])
		print(table)
def Plot(series):
	plot = plottools.PyplotXY()
	if isinstance(series, dict):
		series = series['timeseries']
	plot.addSeries(series)
	plot.render()

def testDataset():
	subject_code = 'SP.POP.TOTL'
	databox = Databox()
	timer = timetools.Timer()
	left_dataset = Dataset('World Development Indicators')
	right_dataset= Dataset('World Economic Outlook')
	#dataset._subjectList()
	
	left_criteria = [('countryCode', 'USA'), ('subjectCode', 'SP.POP.TOTL')]
	right_criteria= [('countryCode', 'USA'), ('subjectCode', 'LP')]
	left = left_dataset.request(left_criteria)
	right= right_dataset.request(right_criteria)
	#left = left['timeseries']
	#right= right['timeseries']
	#dataset._subjectList()
	#comparison = databox(left, right, 'ratio')
	#pprint(comparison)
	#Plot(comparison)
	ComparisonTable(left = left, right = right)
def testDatasets():
	left_criteria = [('countryCode', 'AAA'), ('subjectCode', subject_code)]

EQUIVILANT_CODES = {
	'population': {
		'World Development Indicators': 'SP.POP.TOTL',
		'World Economic Outlook': 'LP'
	}
}

if __name__ == "__main__" and True:
	testDataset()

