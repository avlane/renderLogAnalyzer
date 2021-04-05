from datetime import datetime
from datetime import timedelta
import os

def resetTemplog():
    '''function is used delete the templog from previous execution if it exists
        Takes: no params
        Returns: success (1) or error (0)'''
    try:
        os.remove('temp.log')
        return 1
    except OSError:
        return 0

def countEntries (term):
    '''function counts the number of lines that match a given string
        Takes: search term (str)
        Returns: count (int)'''
    with open("temp.log") as f:
        count = 0
        for line in f:
            if line.__contains__(term):
                count = count +1
            else:
                pass
    return(count)

def generateDicts(log_fh):
    '''function is used to parse every timestamp recorded in the error log and convert it into seconds since epoch
        Takes: log to be converted (file obj)
        Returns: dictionary that contains timestamps and conversions to time since epoch (dict)'''
    currentDict = {}
    for line in log_fh:
        if line.startswith('time='):
            if currentDict:
                yield currentDict
            timestampStr = line[6:29]
            tsDatetime = datetime.strptime(timestampStr, '%Y-%m-%d %H:%M:%S.%f')
            epoch = datetime.utcfromtimestamp(0)
            sinceEpochSeconds = int((tsDatetime - epoch).total_seconds())
            sinceEpochMinutes = int(sinceEpochSeconds/60)
            sinceEpochHours = int(sinceEpochMinutes/60)
            currentDict = {'timestamp': tsDatetime,'sinceEpochSeconds': sinceEpochSeconds,'sinceEpochMinutes':sinceEpochMinutes,'sinceEpochHours':sinceEpochHours}
    yield currentDict

def eventDensity(listEvents, unit):
    '''function is used to calculate number of events in each hour and minute seperately
        Takes: list of events and a unit that it should calculate for (hours or minutes)
        Returns: dictionary of unique events (key) and a count for occurences of each (value)'''
    if (unit == 'hour'):
        dictUnique = unique(listEvents,'sinceEpochHours')
        for event in listEvents:
            for dict in dictUnique:
                if (dict == event['sinceEpochHours']):
                    dictUnique[dict] += 1
    if (unit == 'minute'):
        dictUnique = unique(listEvents,'sinceEpochMinutes')
        for event in listEvents:
            for dict in dictUnique:
                if (dict == event['sinceEpochMinutes']):
                    dictUnique[dict] += 1
    return dictUnique

def unique(listEvents,unit):
    '''function compiles a dictionary of unique values from a list
        Takes: list of events (list)
        Returns: dictionary of unique events an a zeroed counter value for each (dict)'''
    # intilize a null list
    unique_list = []
    BucketsDict = {}
    # traverse for all elements
    for event in listEvents:
        # check if exists in unique_list or not
        if event[unit] not in unique_list:
            unique_list.append(event[unit])
    #extract each item in the list and add it to a new dictionary and assign each key a value of 0
    for line in unique_list:
        tempBucketsDict= {int(line): 0}
        BucketsDict.update(tempBucketsDict)
    return BucketsDict

def mergeFiles(listFile):
    '''function to merge all error log files into one for simplified searching
        Takes: list of files (list)
        Returns: no params'''
    #iterate through all files in the list, merge it into the newly created temp.log
    for file in listFile:
        firstfile = 'temp.log'
        secondfile = file
        f1 = open(firstfile, 'w+')
        f2 = open(secondfile, 'r')
        f1 = open(firstfile, 'a+')
        f2 = open(secondfile, 'r')
        # appending the contents of the second file to the first file
        f1.write(f2.read())
        # relocating the cursor of the files at the beginning
        f1.seek(0)
        f2.seek(0)
        # closing the files
        f1.close()
        f2.close()
    return

def searchMostEventful():
    '''function searches for hour and the minute across all logs that had the most entries (seperately)
        Takes: no params
        Returns: list a two item list with timestamp of most eventful hour and timestamp for most eventful minute (list)'''  
    with open("temp.log") as f:
        listEvents= list(generateDicts(f))
        hourBuckets = eventDensity(listEvents, 'hour')
        mostEventfulHour = datetime.fromtimestamp((max(hourBuckets, key=hourBuckets.get))*60*60)
        minuteBuckets = eventDensity(listEvents, 'minute')
        mostEventfulMinute = datetime.fromtimestamp((max(minuteBuckets, key=minuteBuckets.get))*60)
        results = [mostEventfulHour, mostEventfulMinute]
    return(results)

def main():
    resetTemplog()
    mergeFiles(['api-75fd6d7c7d-7qsr5.log', 'api-75fd6d7c7d-brhdl.log', 'api-75fd6d7c7d-nptc9.log'])
    mostEventful = searchMostEventful()
    totalErrors = str(countEntries('level=error'))
    totalInfo = str(countEntries('level=info'))
    totalEntries = str(countEntries('time'))
    print('Across all files:')
    print('Total Entries: ' + totalEntries)
    print('HOUR with most entries: ' + str(mostEventful[0]))
    print('MINUTE most entries: ' + str(mostEventful[1]))
    print('Total Error-level entries: ' + totalErrors)
    print('Total Info-level entries: ' + totalInfo)

main()