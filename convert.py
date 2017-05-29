import math
import scipy


class Databox:
	""" Processes a list of year-value pairs.
	"""
	def __init__(self):
		pass
	def __call__(self, series, key):
		"""
		"""
		
	@staticmethod
	def _convertSeries(series, tolist = True):
		""" Converts a scipy.interpolate.interpolate.interp1d object 
			to a list
			Parameters
			----------
				series: list<int,number> OR scipy.interpolate.interpolate.interp1d
					The series to convert
				tolist: bool; default True
					If true, converts the input to list<year:value>
			Returns
			----------
				series: list<int,number> OR scipy.interpolate.interpolate.interp1d
		"""
		if not isinstance(series, list) and tolist:
			series = [(i, series(i)) for i in range(min(series.x), max(series.x))]
		elif isinstance(series, list) and not tolist:
			x, y = zip(*series)
			series = interp1d(x, y, kind = 'linear', bounds_error = False, fill_value = (y[0], y[-1]))
		return series

	def compare(self, key, sub, kind = 'Ratio'):
		""" Compares two series against each other
			Parameters
			----------
				key: list<int,number> OR scipy.interpolate.interpolate.interp1d
					The series all otehr series will be compared against
				sub: list<int,number> OR scipy.interpolate.interpolate.interp1d
					The series to compare
				kind: {'Ratio', 'Difference'}; default 'Ratio'
					The kind of comparison to perform
					'Ratio': 'sub' / 'key'
					'Difference': 'key' - 'sub'
			Returns
			----------
				series: list<tuple<int,number>>
					The calculated comparison
		"""
		key = self._convertSeries(key, tolist = False)
		sub = self._convertSeries(sub)

		series = list()
		for year, value in sub:
			kvalue = key(year)
			if kind == 'Ratio':
				v = value / kvalue
			else: v = kvalue - value
			series.append((year, v))
		return series
	
	def doublingTime(self, series, kind = 'Time'):
		""" Calculates the time required for a data series to double in value,
			based on the difference between the start year and end year.
			Parameters
			----------
				series: list<int:number> OR scipy.interpolate.interpolate.interp1d
					The series to calculate the doubling time for.
				kind: {'Time', 'Year'}; default 'Year'
					'Time': The total time for the series to double in value
					'Year': The year the series will double in value
			Returns
			----------
				series: list<tuple<int,number>>
		"""
		series = self._convert_series(series)

		series = list(zip(x, y))
		doubling_series = list()
		for point1, point2 in zip(series[:-1], series[1:]):
			y1, v1 = point1
			y2, v2 = point2
			Td = ((y2-y1) * math.log(2)) / math.log(v2/v1)
			doubling_series((y2, Td))
		return doubling_series

		if kind == 'Year':
			doubling_series = [(i, j + i) for i, j in doubling_series]
		return doubling_series
	
	def yearlyChange(self, series, kind = 'Growth'):
		""" Calculates year-on-year changes in the series
			Parameters
			----------
				series: list<int:number> OR scipy.interpolate.interpolate.interp1d
					The series to calculate the doubling time for.
				kind: {'Growth', 'Total'}; default 'Growth'
					'Growth': The percent change from year-to-year
					'Total': The absolute change between years
			Returns
			----------
				series: list<tuple<int,number>>
		"""
		
		series = self._convert_series(series)

		newseries = list()
		for point1, point2 in zip(series[:-1], series[1:]):
			y1, v1 = point1
			y2, v2 = point2

			if kind == 'Growth':
				value = (v2-v1)/v1
			else:
				value = v2 - v1
			newseries.append((y2, value))

	def reset(self):
		self.__init__()
