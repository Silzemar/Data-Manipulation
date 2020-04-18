from functions import *

from datetime import datetime, timedelta

# -------------------- ASSIGN FILES URL -------------------- #
( confirmedCasesUrl
, recoveredCasesUrl
, deathsCasesUrl
, populationByCountryUrl
, countryCodesUrl) = getCasesUrl()

# -------------------- LOAD THE DATASETS -------------------- #
( confirmedCasesDataSet
, recoveredCasesDataSet
, deathsCasesDataSet
, populationByCountryDataSet
, countryCodesDataSet) = getDataSets(confirmedCasesUrl
                                   , recoveredCasesUrl
                                   , deathsCasesUrl
                                   , populationByCountryUrl
                                   , countryCodesUrl)

# -------------------- JOIN THE DATASETS WITH COUNTRY CODES DATASET -------------------- #
( confirmedCasesDataSet
, recoveredCasesDataSet
, deathsCasesDataSet) = mergeDataSetsWithCountryCodes(confirmedCasesDataSet
                                                    , recoveredCasesDataSet
                                                    , deathsCasesDataSet
                                                    , countryCodesDataSet)

# -------------------- MELT THE DATASETS -------------------- #
( confirmedCasesMelted
, recoveredCasesMelted
, deathsCasesMelted
, populationByCountryMelted) = melfDataSets(confirmedCasesDataSet
                                          , recoveredCasesDataSet
                                          , deathsCasesDataSet
                                          , populationByCountryDataSet)

# -------------------- JOIN THE DATASETS -------------------- #
joinedDataSet = joinDataSets(confirmedCasesMelted
                           , recoveredCasesMelted
                           , deathsCasesMelted)

# -------------------- MERGE THE DATASET JOINED WITH POPULATION BY COUNTRY DATASETS -------------------- #
joinedDataSet = mergeFilesWithPopulationByCountryDataSets(joinedDataSet
                                                        , populationByCountryMelted[['Population', 'Country_Code']],\
                                                                                     'Country_Code', 'Country_Code')

# ITEM 2 ------------------------------------------------------------------------------------------------------------------------ #

# -------------------- SHOWS HOW MANY COUNTRIES HAVE REPORTED AT LEAST 10 CASES -------------------- #
print(getHowManyReportedAtLeastTenCases(joinedDataSet))

# -------------------- SHOWS 5 COUNTRIES WITH HIGEST ACTIVE CASES -------------------- #
print(getFiveCountriesWithHighestCases(joinedDataSet))

# -------------------- SHOWS THE CURRENT RATE OF INCREASE IN THE TOTAL NUMBER OF CASES, BASED ON THE LAST WEEK OF DATA -------------------- #
print(getRateBasedInTheTotalNumber(joinedDataSet))

# ITEM 3 ------------------------------------------------------------------------------------------------------------------------ #

#GETS THE DATAS GROUPING PER COUNTRY AND SELECTING THE DATE JUST THE FIRST DATE THE CONTRY HAVE MORE THAN 10 CASES
countryFromStartedDateDataSet = groupCountryPerDateZero(joinedDataSet)

# ITEM 4 ------------------------------------------------------------------------------------------------------------------------ #

# GETS THE REGION WHERE THE CASES STARTED PER COUNTRY
regionWhereCasesStarted = getRegionWhereCasesStarted(countryFromStartedDateDataSet)

#GETS THE INTERVAL BEETWEEN INITIAL DATE AND PICK DATE
howManyDaysBetweenStartAndPickDataSet = getHowManyDaysBetweenStartAndPick(countryFromStartedDateDataSet)

#The dataset does not contain any information at the level of individual cases. However, based on the
#available data, can you estimate how long it takes patient to recover? Does this vary by region or
#country? How confident can you be about these results?

# GETS SUM OF ACTIVE AND CONFIRMED PER COUNTRY AND DATE
activeAndConfirmedPerCountryDataSet = getflatteningPerCountry(countryFromStartedDateDataSet) 

#SHOWS THE CFR PER COUNTRY
cFRPerCountryPerCountry = getCFRPerCountry(countryFromStartedDateDataSet)

#SHOWS THE SUM OF CONFIRMED, RECOVERED, DEATHS, ACTIVE AND CFR PER COUNTRY
print(getCFRPerCountry(countryFromStartedDateDataSet))

# ITEM 5 ------------------------------------------------------------------------------------------------------------------------ #

# GETS THE SUM OF CONFIRMED, RECOVERD, DEATHS AND ACTIVE COUNTYES FOR COUNTRIES WITH AT LEAST 50 CONFIRMED CASES
sumOfCasesPerCountryAndDateDataSet = getSumOfCasesPerCountryAndDate(countryFromStartedDateDataSet)



 




