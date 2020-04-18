import pandas as pd

# RETURNS THE URL OF THE FILES
def getCasesUrl():
    confirmedCasesUrl = "time_series_19-covid-Confirmed.csv"
    recoveredCasesUrl = 'time_series_19-covid-Recovered.csv'
    deathsCasesUrl = 'time_series_19-covid-Deaths.csv'
    populationByCountryUrl = '._population-figures-by-country-csv_csv.csv'
    countryCodesUrl = 'country_codes.csv'
    return (confirmedCasesUrl
          , recoveredCasesUrl
          , deathsCasesUrl
          , populationByCountryUrl
          , countryCodesUrl)

# LOADS A DATASET BY ITS URL
def setDataSet(urlCasesFiles): 
    return pd.read_csv(urlCasesFiles)

# LOADS THE DATASETS
def getDataSets(confirmedCasesUrl
              , recoveredCasesUrl
              , deathsCasesUrl
              , populationFiguresByCountryUrl
              , countryCodesUrl):
    confirmedCasesDataSet = setDataSet(confirmedCasesUrl)
    recoveredCasesDataSet = setDataSet(recoveredCasesUrl)
    deathsCasesDataSet    = setDataSet(deathsCasesUrl)
    populationByCountry = setDataSet(populationFiguresByCountryUrl)
    countryCodesDataSet = setDataSet(countryCodesUrl)
    return confirmedCasesDataSet, recoveredCasesDataSet, deathsCasesDataSet, populationByCountry, countryCodesDataSet

# MELTS THE DATASETS
def melfDataSet(dataSet, id_vars, columns):
    dataSetMelted = dataSet.melt(id_vars = id_vars)
    dataSetMelted.rename(columns = columns, inplace=True)
    return dataSetMelted

# MELTS THE DATASETS AND CHANGE THE NAME OF COLUMNS VARIABLE AND VALUE
def melfDataSets(confirmedCasesDataSet
               , recoveredCasesDataSet
               , deathsCasesDataSet
               , populationByCountryDataSet):
    id_vars = ['Province/State', 'Country', 'Country_Code','Lat', 'Long']

    columns = {'Province/State' : 'Region', 'variable' : 'Date', 'value' : 'Confirmed'}
    confirmedCasesMelted = melfDataSet(confirmedCasesDataSet, id_vars, columns)

    columns = {'Province/State' : 'Region','variable' : 'Date', 'value' : 'Recovered'}
    recoveredCasesMelted = melfDataSet(recoveredCasesDataSet, id_vars, columns)

    columns = {'Province/State' : 'Region','variable' : 'Date', 'value' : 'Deaths'}
    deathsCasesMelted = melfDataSet(deathsCasesDataSet, id_vars, columns)   

    id_vars = ['Country', 'Country_Code']
    columns = {'variable' : 'Year', 'value' : 'Population'}

    populationByCountryMelted = melfDataSet(populationByCountryDataSet, id_vars, columns)
    # SELECT JUST THE POPULATION OF THE LASTEST SENSUS #
    populationByCountryMelted = populationByCountryMelted[populationByCountryMelted.Year == "Year_2016"]

    return (confirmedCasesMelted
          , recoveredCasesMelted
          , deathsCasesMelted
          , populationByCountryMelted)

# JOINS THE DATASETS
def joinDataSets(confirmedDataSets, recoverDataSets, deathsDataSets):
    dataset = confirmedDataSets.join(recoverDataSets['Recovered'])
    dataset = dataset.join(deathsDataSets['Deaths'])
    return dataset

# SAVES A DATASET AS CSV
def saveAsCsvFile(dataSet, fileName):
    dataSet.to_csv(fileName)

# MERGES DATASETS (LEFT JOIN)
def mergeFilesInnerJoin(dataSet1, dataSet2, field1, field2):
    return pd.merge(left=dataSet1, right=dataSet2, how='left', left_on=field1, right_on=field2)

# MERGES THE DATASETS WITH POPULATION BY COUNTRY DATASETS (LEFT JOIN)
def mergeFilesWithPopulationByCountryDataSets(dataSet1, dataSet2, field1, field2):
    dataSet = mergeFilesInnerJoin(dataSet1, dataSet2, field1, field2)

    # -------------------- ALTER THE EMPTY CELLS FROM REGION TO UNKNOW -------------------- #
    dataSet = alterEmptyRegions(dataSet)

    # --------------------  -------------------- #
    dataSet = convertDateTime(dataSet)

    dataSet = dataSet.sort_values(by=['Country', 'Date'], ascending=[True, False])

    return dataSet

# NORMALIZES THE DATAS GROUPING PER COUNTRY AND SELECTING THE DATE JUST THE FIRST DATE THE CONTRY HAVE MORE THAN 10 CASES
def groupCountryPerDateZero(dataSet):
    firstCasesDataSet = dataSet[dataSet['Confirmed'] >= 10].set_index(['Date']).groupby(['Country_Code'])['Confirmed'].nsmallest(1).reset_index()
    dataSet = mergeFilesInnerJoin(firstCasesDataSet, dataSet, 'Country_Code', 'Country_Code')

    dataSet = dataSet.loc[dataSet['Date_y'] >= dataSet['Date_x']]
    dataSet = dataSet.drop(['Confirmed_x'], axis=1)
    dataSet = dataSet.rename(columns={'Date_x' : "Date0", "Date_y": "Date", "Confirmed_y": "Confirmed"})
    dataSet['Active'] = dataSet['Confirmed'] - (dataSet['Recovered'] + dataSet['Deaths'])    
    dataSet = dataSet.sort_values(by=['Country_Code', 'Date', 'Region'], ascending=[True, True, True])
    return dataSet

# MERGES ALL DATASET WITH THE DATASET COUNTRY CODES
def mergeDataSetsWithCountryCodes(confirmedCasesDataSet
                                , recoveredCasesDataSet
                                , deathsCasesDataSet
                                , countryCodesDataSet):
    confirmedCasesDataSet = mergeFilesInnerJoin(confirmedCasesDataSet, countryCodesDataSet, 'Country/Region', 'Country')
    confirmedCasesDataSet = confirmedCasesDataSet.drop(['Country/Region'], axis=1)

    recoveredCasesDataSet = mergeFilesInnerJoin(recoveredCasesDataSet, countryCodesDataSet, 'Country/Region', 'Country')
    recoveredCasesDataSet = recoveredCasesDataSet.drop(['Country/Region'], axis=1)

    deathsCasesDataSet  = mergeFilesInnerJoin(deathsCasesDataSet, countryCodesDataSet, 'Country/Region', 'Country')
    deathsCasesDataSet = deathsCasesDataSet.drop(['Country/Region'], axis=1)

    return (confirmedCasesDataSet
          , recoveredCasesDataSet
          , deathsCasesDataSet)

# ALTERS ALL EMPTY CELLS TO UNKNOW
def alterEmptyRegions(dataSet):
    return dataSet.fillna("Unknow")

# RETURNS HOW MANY COUNTRIES HAVE REPORTED AT LEAST 10 CASES
def getHowManyReportedAtLeastTenCases(dataSet):
    return dataSet[dataSet['Confirmed'] >= 10].groupby('Country')['Confirmed'].sum().count()

# RETURNS THE 5 COUNTRIES WITH HIGEST CASES
def getFiveCountriesWithHighestCases(dataSet):
    dataSet = dataSet.groupby('Country')['Confirmed'].sum() - (dataSet.groupby('Country')['Recovered'].sum()
                                                             + dataSet.groupby('Country')['Deaths'].sum())
    dataSet = dataSet.groupby('Country').sum().nlargest(5)
    return dataSet

# CONVERTS FIELD DATE TO FORMAT MM-DD-YYYY
def convertDateTime(dataSet):
    dataSet['Date'] =  pd.to_datetime(dataSet['Date'], errors='coerce')
    return dataSet

# RETURNS THE RATE OF INCREASE IN THE TOTAL NUMBER OF CASES, BASED ON THE LAST WEEK OF DATA
def getRateBasedInTheTotalNumber(dataSet):
    numberOfCasesWeekMax = dataSet[dataSet['Date'].dt.week == dataSet['Date'].dt.week.max()]['Confirmed'].sum()
    totalNumberOfCase = dataSet['Confirmed'].sum()
    result = totalNumberOfCase - numberOfCasesWeekMax
    result = result / totalNumberOfCase * 100

    resultText = 'Increase of '
    result = resultText + "%.0f%%" % result
    return result

# RETURNS THE REGION WHERE THE CASES STARTED PER COUNTRY
def getRegionWhereCasesStarted(dataSet):
    dataSet = dataSet.loc[(dataSet['Date'] == dataSet['Date0']) & dataSet['Confirmed'] > 0]
    dataSet = dataSet.sort_values(by=['Country', 'Confirmed'], ascending=[True, False])
    return dataSet

# RETURNS THE INTERVAL BEETWEEN INITIAL DATE AND PICK DATE PER COUNTRY
def getHowManyDaysBetweenStartAndPick(dataSet):
    firstCasesDataSet = dataSet[dataSet['Confirmed'] >= 10].set_index(['Date']).groupby(['Country_Code', 'Country'])['Confirmed'].nsmallest(1).reset_index()
    pickCasesDataSet = dataSet.set_index(['Date']).groupby(['Country_Code', 'Country'])['Confirmed'].nlargest(1).reset_index()
    
    dataSet = mergeFilesInnerJoin(firstCasesDataSet, pickCasesDataSet, 'Country_Code', 'Country_Code')

    dataSet['Interval'] = dataSet['Date_y'] - dataSet['Date_x']
    dataSet = dataSet.rename(columns={'Date_x' : "First_Cases_Date", "Date_y": "Pick_Date", "Country_x" : "Country"})
    dataSet = dataSet.drop(['Confirmed_x', 'Confirmed_y', "Country_y"], axis=1)
    return dataSet

# RETURNS THE SUM OF ACTIVE AND CONFIRMED CASES PER COUNTRY AND DATE
def getflatteningPerCountry(dataSet):
    dataSet = dataSet.groupby(['Country', 'Date'])['Confirmed', 'Active'].sum()
    dataSet = dataSet.sort_values(by=['Country'], ascending=[True])
    return dataSet

# RETURNS THE CFR PER COUNTRY
def getCFRPerCountry(dataSet):
    dataSet = dataSet.groupby('Country')['Confirmed', 'Recovered', 'Active', 'Deaths'].sum()
    dataSet['CFR'] = dataSet['Deaths'] / (dataSet['Deaths'] + dataSet['Recovered'])
    dataSet = dataSet.sort_values(by=['Country'], ascending=[True])
    return dataSet

#  RETURNS THE SUM OF CONFIRMED, RECOVERD, DEATHS AND ACTIVE COUNTYES FOR COUNTRIES WITH AT LEAST 50 CONFIRMED CASES
def getSumOfCasesPerCountryAndDate(dataSet):
    dataSet = dataSet[dataSet['Confirmed'] >= 50].groupby(['Country', 'Date', 'Population'])['Confirmed', 'Recovered', 'Deaths', 'Active'].sum()
    dataSet.sort_values(by=['Country', 'Date'], ascending=[True, False])
    return dataSet
