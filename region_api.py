import os
import __init__

from pprint import pprint
import pandas
from prettytable import PrettyTable
#from databox import Databox

#Requires __init__.py to add the gihub folder to the PATH variable.
import pytools.tabletools as tabletools
import pytools.numbertools as numbertools
import pytools.plottools as plottools

DATA_FOLDER = "D:\\Proginoskes\\Documents\\Data\\Harmonized Data\\"
if not os.path.exists(DATA_FOLDER):
    DATA_FOLDER = os.path.expanduser("~\\Google Drive\\Harmonized Data\\")


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

        kwargs['sheetname'] = self.configuration.get('sheetname', 0)

        super().__init__(filename, **kwargs)
    @staticmethod
    def _getDatasetConfiguration(name):
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
                'name': 'Historical Country Profiles',
                'filename': os.path.join(DATA_FOLDER, "World", "Historical Country Population and GDP.xlsx"),
                'keyRegionCodeColumn': "countryCode",
                'sheetname': "Combined"
            },
            {
                'name': 'World Development Indicators',
                'filename': os.path.join(DATA_FOLDER,
                                         "Global Tables", "World Development Indicators.txt"),
                'keyRegionCodeColumn': 'countryCode'

            },
            {
                'name': 'World Economic Outlook',
                'filename': os.path.join(DATA_FOLDER, "Global Tables", "World Economic Outlook.xlsx"),  # Filename
                'keyRegionCodeColumn': 'countryCode'  # The column that defines the region codes.
            },
            {
                'name': "USA City Populations",
                'filename': os.path.join(DATA_FOLDER, "Country Tables\\United States\\1790-2010_MASTER.csv"),
                'keyRegionCodeColumn': 'cityStName'
            }
        ]
        configurations = {i['name']: i for i in configurations}

        return configurations[name]
    @staticmethod
    def _catagorizeColumns(columns):
        """ Separates columns into timeseries or other
        """
        result = {'timeseries': [], 'dataseries': []}
        for column in columns:
            is_number = numbertools.isNumber(column) or column.isdigit()

            if is_number:
                result['timeseries'].append(column)
            else:
                result['dataseries'].append(column)

        return result
    @staticmethod
    def _getScaleMultiplier(value):
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
    @staticmethod
    def _getIdentifierFields(series):
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
                column = numbertools.toNumber(column)
                if 'scale' in series:
                    multiplier = self._getScaleMultiplier(series['scale'])
                    value *= multiplier
                timeseries.append((column, value))
            else:
                other_data.append((column, value))
        other_data = dict(other_data)

        if len(timeseries) > 0:
            other_data['timeRange'] = [min(timeseries, key=lambda s: s[0])[0],
                                       max(timeseries, key=lambda s: s[0])[0]]
            other_data['dataRange'] = [min(timeseries, key=lambda s: s[1])[1],
                                       max(timeseries, key=lambda s: s[1])[1]]
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
            if row[subject_code_column] in seen:
                continue
            else:
                seen.append(row[subject_code_column])
                subject_code = row[subject_code_column]

                if subject_name_column in row:
                    subject_name = row[subject_name_column]
                else:
                    subject_name = ""

                if subject_description_column in row:
                    subject_text = row[subject_description_column]
                else:
                    subject_text = ""
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
        print(left['countryCode'])
        left_timeseries = left['timeseries']
        right_timeseries = right['timeseries']
        left_timeseries = dict(left_timeseries)
        right_timeseries = dict(right_timeseries)
        table = PrettyTable(field_names=['year', left['countryCode'], right['countryCode'], 'ratio'])
        # help(table)
        for year in left_timeseries.keys():
            lv = left_timeseries[year]
            rv = right_timeseries.get(year)
            r = lv / rv
            table.add_row([year, lv, rv, r])
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
    # timer = timetools.Timer()
    left_dataset = Dataset('Historical Country Profiles')
    # right_dataset= Dataset('World Economic Outlook')
    # dataset._subjectList()

    left_criteria = [('countryCode', 'GBR'), ('subjectCode', 'GDP')]
    right_criteria = [('countryCode', 'FRA'), ('subjectCode', 'GDP')]
    left = left_dataset.request(left_criteria)
    right = left_dataset.request(right_criteria)
    # left = left['timeseries']
    # right= right['timeseries']
    # dataset._subjectList()
    comparison = databox(left['timeseries'], right['timeseries'], 'ratio')
    # pprint(comparison)
    # Plot(comparison)
    ComparisonTable(left=left, right=right)


EQUIVILANT_CODES = {
    'population': {
        'Historical Country Profiles': 'POP',
        'World Development Indicators': 'SP.POP.TOTL',
        'World Economic Outlook': 'LP'
    },
    'gross domestic product': {
        'Historical Country Profiles': 'GDP'
    },
    'gross domestic product per capita': {
        'Historical Country Profiles': 'GDPPC'
    }
}

if __name__ == "__main__" and True:
    testDataset()
