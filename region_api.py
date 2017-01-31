import os
import pandas
import configparser
from fuzzywuzzy import process
from pprint import pprint
from scipy.interpolate import interp1d
from Table import Table

human_readable = {
	'UN City Populations': "WUP2014-F11b-30_Largest_Cities_in_2014_by_time.xlsx",
	'US City Populations': "1790-2010_MASTER.xlsx",
	'State GDP to 1997': "BEA State GDP up to 1997.xlsx",
	'Historical City Populations': "chandlerV2.xlsx"
}

configuration = {
	"1790-2010_MASTER.xlsx": { #US city proper populations by decade
		'region column': "CityST",
		'key column': 'Year',
		'value column': 'Population',
		'format': ['int', 'int'], #[OPTIONAL] The variable type of the time series response
		'sheetname': 'Flattened', #[OPTIONAL]
		'skiprows': 0
	},
	"WUP2014-F11b-30_Largest_Cities_in_2014_by_time.xlsx": { #UN list of 30 largest cities with pop estimates from 1950 to 2030
		'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\World\\Urban Areas\\WUP2014-F11b-30_Largest_Cities_in_2014_by_time.xlsx",
		'region column': "Urban Agglomeration",
		'key column': "Year",
		'value column': "Population (millions)",
		'format': ['int', 'int'],
		'multiplier': 'millions', #Used to convert truncated values to absolute values
		'sheetname': 'DATA',
		'skiprows': 16
	},
	"chandlerV2.xlsx": {
		'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\World\\Historical Urban Populations\\chandlerV2.xlsx",
		'region column': 'City-Country',
		'key column': "IntYear",
		'value column': 'Population',
		'format': ['str', 'int'],
		'sheetname': 'Flattened'
	},
	"BEA State GDP up to 1997.xlsx": {
		'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\United States\\Economic\\BEA State GDP up to 1997.xlsx",
		'region column': "Region Name",
		'key column': "Year",
		'value column': 'GDP',
		'format': ['int', 'int'],
		'multiplier': 'millions',
		'sheetname': 'Flattened',
		'region columns': ["Region Code"] #A list of columns searchable when searching for a region code/name
	},
	"National Statistics 1AD - 2008AD.xlsx": {
		'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\Historical Datasets\\National Statistics 1AD - 2008AD.xlsx",
		'region column': 'Region',
		'key column': "Year",
		'value column': 'Population',
		'format': ['int', 'int'],
		'multiplier': 'thousands',
		'sheetname': ['Flattened Population', 'Flattened GDP'],
		'region columns': ['Code'],
	}
}


class API:
	def __init__(self):
		pass

class Dataset(Table):
	def __init__(self, filename, ID = 0, sheet = None):
		if not os.path.exists(filename): filename = configuration[filename]['filename']




		self.config = configuration[os.path.basename(filename)]
		#pprint(self.config)
		if 'sheetname' in self.config: sheetname = self.config['sheetname']
		else: sheetname = None
		if sheetname is None: sheetname = 0
		elif isinstance(sheetname, str): sheetname = sheetname
		elif sheet is not None:
			s = [i for i in sheetname if i == sheet]
			if len(s) == 1: sheetname = i.pop()
			else: sheetname = sheetname[0]
		else:
			sheetname = sheetname[0]


		super().__init__(io = filename, ID = self.config.get('sheetname', 0), skiprows = self.config.get('skiprows'))
		
		self.region_columns = self.config.get('region columns')
		if not isinstance(self.region_columns, list): self.region_columns = [self.region_columns]

		try:
			region_column = self.config['region column']
			self.unique_regions = set(self.df[region_column].values)
		except Exception as exception:
			print(exception)
			for index, row in self.df.iterrows():
				print(index, row)
				if index > 100: break

	def request(self, region, region_columns = None, subject = None, fuzzy = True, ratio = 90, interp = False, normalize = True):
		""" Re-formats a DataFrame to resemble an api.
			Parameters
			----------
				region: string
					The region to search for.
				subject: tuple<string> [(column, value)]
					This uses the values from another column to filter the series. Most useful
					for datasets with more than one subject.
				region_columns: string; default None
					If the region cannot be found, the columns listed here will be used as a fallback.
				fuzzy: bool; default True
					If the selected region can't be found, the closest match with a ratio greater than 90
					using a fuzzy search will be used instead.
				ratio: int; default 90
					The minimum ratio a match must pass if the selected region can't be found.
				interp: bool; default False
					If true, will return a scipy.interpolate.interpolate.interp1d object instead.
				normalize: bool; default False
					Indicates whether to normalize the values to their absolute amount (rather than in, say, 'millions')

			Returns
			-------
				response: dict()
		"""
		if region_columns is None: region_columns = self.region_columns
		match = self.search(region)
		region_column = self.config['region column']
		if match is None:
			for column in region_columns:
				print(column)
				match = self.search(region, column = column)
				if match is not None:
					region = match
					region_column = column
					break
			else:
				region = None
				region_column = self.config.get('region column')

		print(region, region_column)

		time_column = self.config['key column']
		value_column= self.config['value column']

		series = self(region_column, region)
		if subject is not None and subject[1] in series[subject[0]].values:
			subject_column, subject_value = subject
			series = [series[subject_column] == subject_value]


		default_row = series.iloc[0] #Used to populate all static fields.
		static_columns = [i for i in default_row.index if i not in {time_column, value_column}]

		time_series = list(zip(series[time_column].values, series[value_column].values))
		if 'format' in self.config or 'multiplier' in self.config:
			if not normalize: multiplier = None
			else: multiplier = self.config.get('multiplier')
			time_series = self._format_2D_values(time_series, self.config.get('format'), multiplier)

		response = {c:default_row[c] for c in static_columns}
		response['region column'] = region_column
		response['key column'] = time_column
		response['value_column']= value_column
		response['format'] = self.config.get('format')

		if interp:
			x, y = zip(*time_series)
			time_series = interp1d(x, y, kind = 'linear', bounds_error = False, fill_value = (y[0], y[-1]))
		response['data'] = time_series

		return response

	def _format_2D_values(self, xy, formatter, multiplier):
		""" Formats a 2D array.
			Parameters
			----------
				xy: list<<tuple>>
					A @D series of values
				formatter: list<string>
					A list of data types to convert to.
					Ex. ['int', 'float']
		"""
		#print("_format_2D_values({0}, {1}, '{2}')".format(len(xy), formatter, multiplier))

		invalid_format = formatter is None or len(formatter) == 0
		
		if multiplier is None and invalid_format: return xy
		elif multiplier is not None and invalid_format: formatter = [None]
		if len(formatter) == 1: left, right = formatter[0], None
		else: left, right, *_ = formatter

		x, y = zip(*xy)

		if multiplier is not None:
			if multiplier == 'thousands': multiplier = 1E3
			elif multiplier == 'millions': multiplier = 1E6
			elif multiplier == 'billions': multiplier = 1E9

		x = self._format_1D_values(x, left, None)
		y = self._format_1D_values(y, right, multiplier)

		xy = list(zip(x, y))
		return xy

	def _format_1D_values(self, x, formatter, multiplier):
		""" Formats a 1D series
			Parameters
			----------
				x: list
					A 1D sequence of values.
				formatter: {'int', 'float', 'str'}; default None
					The variable type to convert to. If None, the unformatted
					sequence will be returned.
			Returns
			-------
				series: list
					A 1D sequence converted to the desired variable type.
		"""
		if multiplier is not None:
			x = [float(i)*multiplier for i in x]
		if formatter == None: pass
		elif formatter == 'int': x = list(map(int,x))
		elif formatter == 'float': x = list(map(float(x)))
		elif formatter in {'str', 'string'}: x = list(map(str,x))


		return x


	def list_regions(self):
		for i in sorted(self.unique_regions):
			print(i)
	def search(self, region, column = None, fuzzy = True, ratio = 90):
		if column is None: column = self.unique_regions
		else: column = self.df[column].values
		if region in column:
			match = region
		elif not fuzzy:
			match = None
		else:
			matched_region, matched_ratio = process.extractOne(region, column, processor=None, scorer=None, score_cutoff=0)
			if matched_ratio >= ratio:
				print("{0} not found, using {1} instead.".format(region, matched_region))
				match = matched_region
			else:
				print("{0} is not present in this dataset".format(region))
				match = None


		return match

if __name__ == "__main__":
	print("Running...")
	filename = "chandlerV2.xlsx"
	dataset = Dataset(filename)
	response = dataset.request('London, United Kingdom', normalize = True)
	pprint(response)

