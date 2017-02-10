import aux_tools
import math
import os
import pandas
import configparser
import collections
from fuzzywuzzy import process
from pprint import pprint
from scipy.interpolate import interp1d
from Table import Table

DEFAULT_RATIO = 90 #The default cuttoff when using fuzzywuzzy.

def tonum(x):
	""" Converts the passed argument to a number dtype """
	if isinstance(x, str):
		if '.' in x:
			x = float(x)
		else:
			x = int(x)
	elif x is None: x = math.nan
	return x


class Dataset(Table):
	def __init__(self, title, ID = 0, sheet = None):
		self.config = self._get_configuration(title)

		sheetname = self._get_sheetname(sheet = sheet)

		filename = self.config['filename']
		super().__init__(io = filename, ID = self.config.get('sheetname', 0), skiprows = self.config.get('skiprows'))
		
		self.region_columns = self._parse_unique_values(self.config['region columns'])
		self._parse_dataset() #Unique to each dataset

		self._child_init() #Only exists as a way for child tables to run their own methods at creation.

	def _parse_dataset(self):
		pass

	def _parse_unique_values(self, region_columns):
		unique_values = dict()
		for col in region_columns:
			unique_values[col] = set(self.df[col].values)
		return unique_values
	def _get_sheetname(self, sheet):
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

			return sheetname
	def _child_init(self):
		pass
	def _convert_horizontal_series(self, series):
		""" Converts a series where the year values exist as column headers into the proper format """
		if isinstance(series, pandas.DataFrame):
			print("The request returned a DataFrame, not a Series!")
			series = series.iloc[0]
		years = [i for i in series.index if (not isinstance(i, str) or i.isdigit())]

		response = {c: series[c] for c in series.index if c not in years}
		timeseries = {(y, series[y]) for y in years}
		response['data'] = timeseries

		return response

	def _format_2D_values(self, xy, formatter, multiplier):
		""" Formats a 2D array.
			Parameters
			----------
				xy: list<<tuple>>
					A 2D series of values formatted as a list of len-2 tuples
					Ex. [(x1, y1), (x2, y2)]
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

		xy = [i for i in xy if not math.isnan(i[1])]
		x, y = zip(*xy)

		if isinstance(multiplier, str):
			if multiplier == 'thousands': multiplier = 1E3
			elif multiplier == 'millions': multiplier = 1E6
			elif multiplier == 'billions': multiplier = 1E9
		elif isinstance(multiplier, (int, float)):
			pass

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

	def _get_metadata(self):
		""" Metadata refers to a description of each column and associated notes """
		try:
			metadata = pandas.read_excel(self.config['filename'], sheetname = 'Metadata')
		except:
			k = lambda: 'Not Available'
			metadata = collections.defaultdict(k)

		return metadata

	def help(self, io = None):
		pass

	def list_regions(self):
		for col, values in self.region_columns.items():
			print(col, len(values))
	
	def search_by_region(self, region, ratio = DEFAULT_RATIO, return_type = 'region'):
		""" Searches the dataset for a region.
			Parameters
			----------
				reegion: string
				ratio: int; default 90
				return_type: {'region', 'series'}; default 'region'
		"""

		region_columns = self.region_columns #A dict of unique values for each region column

		for col, values in region_columns.items():
			matched_region = self._search_column(region, values, ratio = ratio)
			if matched_region is not None:
				matched_column = col
				break
		else:
			matched_column = None
			print("WARNING: '{0}' could not be found.".format(region))

		if return_type == 'series' and matched_column is not None:
			if isinstance(matched_region, list):
				series = [self(matched_column, r[0], flag = True) for r in matched_region]
				series = pandas.concat(series)
			else:
				series = self(matched_column, matched_region[0], flag = True)
		else:
			series = None

		if matched_region is None:
			matched_region = (None, None)

		response = {
			'query region': region,
			'region': matched_region[0],
			'column': matched_column,
			'series': series,
			'ratio': matched_region[1]
		}

		return response

	def _search_column(self, region, column, ratio = DEFAULT_RATIO):
		""" Searches a column for the specified region.
			Parameters
			----------
				region: string
				column: string, list
					A string specifying a column in the dataset, or a list of values to search through.
				ratio: int
			Returns
			---------
				match: string
					Returns the region string, if found. None otherwise.
		"""

		if isinstance(column, str):
			column = self.df[column].values

		if region in column:
			match = (region, 100) #Compatable with the output of process.extractOne, used below.
		else:
			match = process.extractOne(region, column, score_cutoff = ratio)#, limit = 10)
		return match

	@staticmethod
	def _interpolate(series):
		x, y = zip(*series)
		z = interp1d(x, y, kind = 'linear', bounds_error = False, fill_value = (y[0], y[-1]))

		return z

	def request(self, region, *keys, **kwargs):
		""" Requests data from the Dataset.
			Parameters
			----------
				region: string
					A region to search the data set for. Available columns are defined in the configuration settings.
				*keys: string, list<string>
					string: Used with flattened datasets to pair a column with the year values to produce a time-based series.
					A list of strings is used to filter a table were years values are saved as column names, and multiple 
					subjects may be available. Should be formatted as [(column1, value1, column2, value2, etc.)]
				**kwargs: available keyword arguemts.
					* 'interp': bool; default False
						If true, the timeseries will be return as an interpolated object.
					* 'forcelist': bool; default False
						If false, a search that returns a single element will be returned as a pandas.Series.
					* 'domain': number, tuple(number, number); default None
						A specific section of early data to return. If an iterator is passed,
						the maximum and minimum contained values will be used to form the domain,
						else if a number is passed, it will be used as the start of a range.
			Returns
			-------
				response: dict<>, list<dict>

		"""
		if len(keys) == 0:
			#TableDataset._request(region, *criteria = (), **kwargs)
			response = self._request(region, **kwargs)
		elif len(keys) == 1:
			#FlattenDataset._request(region, key, **kwargs)
			response = self._request(region, key = keys[0], **kwargs)
		else:
			#TableDataset._request(region, *criteria, **kwargs)
			response = self._request(region, *keys, **kwargs)

		if kwargs.get('interp', False):
			response['data'] = self._interpolate(response['data'])

		if len(response) == 1 and not kwargs.get('forcelist', False):
			response = response.pop()

		return response

class TableDataset(Dataset):
	""" Parses tables that use column names to store time-series information.
		Ex. State Name, State Code, Report, Census, *years...

		Required Parameters
		-------------------
			'filename': string [PATH]
				path to the dataset
			'region columns': string, list<string>
				Columns containing region names or codes to make searchable.		
	"""
	def _child_init(self):
		pass
	
	@staticmethod
	def _get_configuration(key):
		if os.path.isabs(key):
			key = os.path.basename(key)

		config = {
			"US State Population Projections.xlsx": {
				'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\United States\\Population\\Population Projections\\US State Population Projections.xlsx",
				'region columns': ['State Code', 'State Name'],
				'tags': ['Population Projection', 'US States'],
				'name': "US State Population Projections 2010 - 2040"
			},
			"Annual State Populations.xlsx": {
				'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\United States\\Population\\Annual State Populations.xlsx",
				'region columns': ['State Code'],
				'tags': ['Population', 'US States'],
				'name': "US State Populations 1900 - 2016"
			},
			"1790-2010_MASTER.xlsx": {
				'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\United States\\Population\\historical-us-city-populations-master\\data\\1790-2010_MASTER.xlsx",
				'region columns': ['CityST'],
				'tags': ['City', 'Population', 'Historical'],
				'name': "Population of US Cities 1790 - 2010",
				'format': ['int', 'int']
			},
			"Area by NUTS 3 region.xls": {
				'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\Europe\\Area by NUTS 3 region.xls",
				'region columns': ['GEO', 'GEO(L)/TIME'],
				'tags': ['NUTS-3', 'Area', 'Europe'],
				'name': "Area by NUTS-3 region",
				'skiprows': 9
			},
			"Population on 1 January by age, sex and NUTS 2 region.xls": {
				'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\Europe\\Population on 1 January by age, sex and NUTS 2 region.xls",
				'region columns': ['GEO', 'GEO(L)/TIME'],
				'tags': ['population', 'nuts-2', 'europe'],
				'name': "Population of NUTS-2 Regions",
				'skiprows': 10
			},
			"World City Location and Historical Population.xlsx" : {
				'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\Cities\\World City Location and Historical Population.xlsx",
				'region columns': ['City-Country_Name', 'City-Country_Code'],
				'tags': ['population', 'global', 'city', 'historical'],
				'name': "World City Location and Historical Population",
				'multiplier': 1000,
				'format': ['int', 'int']
			},
			"County Population 1790 - 1990.xls" : {
				'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\United States\\County Population 1790 - 1990.xls",
				'region columns': ['CountyST'],
				'tags': ['population', 'historical', 'county'],
				'name': "US County Populations 1810 - 1990"
			}
		}
		config = config[key]
		print(config['filename'])
		return config

	def _flatten(self, series, domain = None):
		""" Converts a row (in dict form) from a timeseries dataset where years comprise column names.
			Ex. 
		"""

		years = [i for i in series.keys() if isinstance(i, (int, float)) or i.isdigit()]
		if domain is not None:
			if isinstance(domain, (list,tuple)):
				years = [y for y in years if (min(domain) <= tonum(y) <= max(domain))]
			else:
				years = [y for y in years if tonum(y) >= domain]

		time_series = sorted([(y, series[y]) for y in years])
		time_series = self._format_2D_values(time_series, formatter = self.config.get('format'), multiplier = self.config.get('multiplier'))
		response = {k:series[k] for k in series.keys() if k not in years}
		response['data'] = time_series

		return response

	def _request(self, region, *criteria, **kwargs):
		""" 
			Parameters
			----------
				region: string, list<string>
					The region name(s) or code(s) to search for. The availablity of names/codes depends on the specific dataset.
				*criteria: comma-separated list of criteria to filter the table by.
					Should be formatted as column1, criteria1, column2, criteria2, etc.
					Internalle represented as a tuple of strings.
				**kwargs: dict<>
					A number of additional keyword arguements.
					* 'interp': bool; default False
						Whether to interpolate the data series.
					* 'forcelist': bool; default False
						Whether to return a list when only 1 region is found.
					* 'domain': number, tuple(number, number); default None
						A specific section of early data to return. If an iterator is passed,
						the maximum and minimum contained values will be used to form the domain,
						else if a number is passed, it will be used as the start of a range.

		"""
		if isinstance(region, str): region = [region]
		all_series = list()
		for r in region:
			series = self.search_by_region(r, ratio = kwargs.get('ratio', 90), return_type = 'series')['series']
			if series is not None:
				series = series.T.to_dict().values()
				all_series += series
			else:
				print("Could not find {0}. Did you mean".format(r))
				response = self.search_by_region(r, ratio = 80)
				print(response['region'], '\t', response['ratio'])
		response = list()
		for row in all_series:
			response.append(self._flatten(row, domain = kwargs.get('domain')))

		#if len(response) == 1 and not kwargs.get('forcelist', False): response = response[0]
		return response

class FlattenDataset(Dataset):
	@staticmethod
	def _get_configuration(key):
		if os.path.isabs(key):
			key = os.path.basename(key)

		config = {
			"WUP2014-F11b-30_Largest_Cities_in_2014_by_time.xlsx": {
				'filename': "C:\\Users\\Deitrickc\\Google Drive\\Data\\World\\Urban Areas\\WUP2014-F11b-30_Largest_Cities_in_2014_by_time.xlsx",
				'region columns': ['Urban Agglomeration'],
				'time column': 'Year', 
				'tags': ['Population', 'Population Projection', 'Cities'],
				'name': "World Urbanization Prospects: The 2014 Revision",
				'skiprows': 16,
				'multiplier': 1E6,
				'format': ['int', 'int']
			}
		}
		config = config[key]
		print(config['filename'])
		return config
	
	def _flatten(self, series, key):
		time_column = self.config.get('time column', 'Year')
		time_series = list(zip(series[time_column].values, series[key]))
		time_series = self._format_2D_values(time_series, formatter = self.config.get('format'), multiplier = self.config.get('multiplier'))

		first_row = series.iloc[0]

		response = {col: first_row[col] for col in first_row.index if col not in {time_column, key}}
		response['data'] = time_series

		return response

	def _request(self, regions, key, **kwargs):

		response = list()
		for region in regions:
			match = self.search_by_region(region, return_type = 'series')

			series = self._flatten(match['series'], key)

			response.append(series)

		return response




class TimePlot:
	def __init__(self, timeseries):
		import matplotlib.pyplot as plt 

		timeseries = [i['data'] for i in timeseries]
		fig, ax = plt.subplots(figsize = (20,10))
		for ts in timeseries:
			x, y = zip(*ts)
			plt.plot(x, y)

		plt.show()




if __name__ == "__main__":
	print("Running...")

	#Other Sources:
	#	http://unstats.un.org/unsd/snaama/dnllist.asp

	filename = "WUP2014-F11b-30_Largest_Cities_in_2014_by_time.xlsx"
	#dataset = FlattenDataset(filename)
	dataset = TableDataset("World City Location and Historical Population.xlsx")

	response = dataset.request(['London, GBR', "New York-Newark, USA"], forcelist = True)
	#response += dataset2.request(['London, GBR', 'New York-Newark, USA'], domain = 1800, forcelist = True)
	pprint(response)
	#response2= dataset.request('Los Angeles, CA')
	#plot = TimePlot(response)
	
else:
	pass

