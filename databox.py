import math
import scipy.interpolate as interpolate
from prettytable import PrettyTable

class Databox:
    """ Processes a list of year-value pairs.
    """
    def __init__(self):
        pass

    def __call__(self, left, right = None, key = 'yearlyGrowth'):
        """
            Parameters
            ----------
                left: list<year, value>, scipy.interpolate.interp1d
                right: same as left
                key: {'growth', 'total', 'time', 'year', 'ratio', 'difference'}; default 'Ratio'

        """ 
        if key in {'ratio', 'difference'}:
            result = self.compare(left, right, key)
        elif key in {'doublingTime', 'doublingYear'}:
            result = self.doublingTime(left, key)
        elif key in {'yearlyGrowth', 'yearlyChange'}:
            result = self.yearlyChange(left, key)
        else:
            message = "ERROR: '{}' is not a supported key for Databox.__call__".format(key)
            raise KeyError(message)
        return result

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
            series = interpolate.interp1d(x, y, kind = 'linear', bounds_error = False, fill_value = (y[0], y[-1]))
        return series

    def compare(self, key, sub, kind = 'ratio'):
        """ Compares two series against each other
            Parameters
            ----------
                key: list<int,number> OR scipy.interpolate.interpolate.interp1d
                    The series all otehr series will be compared against
                sub: list<int,number> OR scipy.interpolate.interpolate.interp1d
                    The series to compare
                kind: {'ratio', 'difference'}; default 'Ratio'
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
            if kind == 'ratio':
                v = value / kvalue
            else: v = kvalue - value
            series.append((year, v))
        return series
    
    def doublingTime(self, series, kind = 'doublingTime'):
        """ Calculates the time required for a data series to double in value,
            based on the difference between the start year and end year.
            Parameters
            ----------
                series: list<int:number> OR scipy.interpolate.interpolate.interp1d
                    The series to calculate the doubling time for.
                kind: {'doublingTime', 'doublingYear'}; default 'Year'
                    'Time': The total time for the series to double in value
                    'Year': The year the series will double in value
            Returns
            ----------
                series: list<tuple<int,number>>
        """
        series = self._convertSeries(series)

        #series = list(zip(x, y))
        doubling_series = [(math.nan, math.nan)]
        for point1, point2 in zip(series[:-1], series[1:]):
            y1, v1 = point1
            y2, v2 = point2
            Td = ((y2-y1) * math.log(2)) / math.log(v2/v1)
            doubling_series.append((y2, Td))

        if kind == 'doublingYear':
            doubling_series = [(i, j + i) for i, j in doubling_series]
        return doubling_series
    
    def generateTable(self, series):
        """ Generates a table of all available data manipulations.
        """
        keys = ['yearlyGrowth', 'yearlyChange', 'doublingTime', 'doublingYear']
        table = PrettyTable()
        #table.float_format = '.2f'
        table.add_column('Year', [i[0] for i in series])
        table.add_column('original', [i[1] for i in series])
        for key in keys:
            values = self(series, key = key)
            values = [i[1] for i in values]
            table.add_column(key, values)
        table.float_format = '.3'
        print(table)


    def yearlyChange(self, series, kind = 'yearlyGrowth'):
        """ Calculates year-on-year changes in the series
            Parameters
            ----------
                series: list<int:number> OR scipy.interpolate.interpolate.interp1d
                    The series to calculate the doubling time for.
                kind: {'yearlyGrowth', 'yearlyChange'}; default 'yearlyGrowth'
                    'Growth': The percent change from year-to-year
                    'Total': The absolute change between years
            Returns
            ----------
                series: list<tuple<int,number>>
        """
        
        series = self._convertSeries(series)

        newseries = [(series[0][0], 0)]
        for point1, point2 in zip(series[:-1], series[1:]):
            y1, v1 = point1
            y2, v2 = point2

            if kind == 'yearlyGrowth':
                value = (v2-v1)/v1
            else:
                value = v2 - v1
            newseries.append((y2, value))
        return newseries

    def reset(self):
        self.__init__()

