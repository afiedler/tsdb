#loading the library
source('tsdbR.R')
dyn.load('libtsdbR.so')
TSDBopen()

groupID <- TSDBopen_file('/home/patrick/projects/tsdb/btec_10.tsdb','r')
names <- TSDBtimeseries(groupID)
info <- TSDBget_properties(groupID,names[1])

data <- TSDBget_records(
	groupID = groupID,
	startTimestamp = '2009-03-01 00:00:00', 
	endTimestamp = '2009-04-01 00:00:00', 	
	seriesName = names[1])

#creating new TSDB files
newGroupID <- TSDBcreate_file('/home/patrick/projects/tsdb/testing.tsdb',overwritePermission = TRUE)
timeseriesName <- 'myTimeseries'
timeseriesDescription <- 'Created in R'
fields <- list(
	fieldNames=c('expirationDate','strikePrice','interestRate','optionPrice','indicator'),
	fieldTypes=c('int32','double','double','double','string(13)'))

TSDBcreate_timeseries(groupID=newGroupID, timeseriesName=timeseriesName, timeseriesDescription=timeseriesDescription,
	fields=fields)

newNames <- TSDBtimeseries(newGroupID)
newInfo <- TSDBget_properties(newGroupID,newNames[1])

status <- TSDBclose_file(groupID)
status <- TSDBclose_file(newGroupID)
TSDBclose()
dyn.unload('libtsdbR.so')
