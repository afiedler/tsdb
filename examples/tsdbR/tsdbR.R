TSDBopen <- function()
{
	return(.Call('TSDBopen'));	
}

TSDBclose <- function()
{
	return(.Call('TSDBclose'));
}

#function which opens TSDB file
TSDBopen_file <- function(fileName,permission)
{
	return(.Call('TSDBopen_file',fileName,permission))
}

TSDBclose_file <- function(groupID)
{
	return(.Call('TSDBclose_file',groupID))
}

#function closing TSDB file
TSDBclose_file <- function(groupID)
{
	return(.Call('TSDBclose_file',groupID))   
}

#function which returns list of timeseries names within TSDB file
TSDBtimeseries <- function(groupID)
{
	return(.Call('TSDBtimeseries',groupID))
}

#function which returning list of information about particular timeseries
TSDBget_properties <- function(groupID,seriesName)
{
	return(.Call('TSDBget_properties',groupID,seriesName))
}

#function returning milliseconds since epoch
getTimeStamp <- function(date) 
{
	if (typeof(date) == 'character')
	{
		return((as.numeric(as.POSIXct(date,tz='GMT'))-
			as.numeric(as.POSIXct('1970-01-01',tz='GMT')))*1000)
	}
	else if (typeof(date) == 'double')
	{
		return (date)
	}
	else
	{
		stop("Date inputs must be string or double");
	}
}
    
#function retrieving records by timestamps
TSDBget_records <- function(groupID,seriesName,startTimestamp,endTimestamp,columnsWanted=NULL)
{
	return(.Call('TSDBget_records',groupID,seriesName,getTimeStamp(startTimestamp),
		getTimeStamp(endTimestamp),columnsWanted))

}

#function which creates a new TSDB file
TSDBcreate_file <- function(fileName, overwritePermission = FALSE)  
{
    return(.Call('TSDBcreate_file',fileName,overwritePermission))
}

#function adding a new timeseries to existing TSDB file
TSDBcreate_timeseries <- function(groupID,timeseriesName,timeseriesDescription='',fields)
{
    return(.Call('TSDBcreate_timeseries',groupID,timeseriesName,timeseriesDescription,fields))
}

#function to append records to TSDB timeseries
TSDB_append <- function(groupID,seriesName,data)
{
    tsInfo <- TSDB_ts_info(groupID,seriesName)
    
    #number of columsn in TSDB timeseries
    numColsTS <- length(tsInfo$fieldOffSets)
    
    #checking to see if number of columns between input and TSDB timeseries agree
    if (numColsTS!=length(data))
    {
        stop(sprintf('Mismatch between number of columns.\nThe TSDB timeseries has %d column, while the input has %d.',numColsTS,length(data)))
    }
    
    #checking to see if columns have the same number of observations
    obs <- length(data[[1]])
    for (i in c(2:numColsTS))
    {
        if (obs!=length(data[[i]]))
        {
            stop(sprintf('Observation count mismatch.\nTimestamp column has %d observations.\nColumn %d has %d observations.\n',obs,i,length(data[[i]])))
        }
    }
            
    
    #checking to see if data types are in agreement
    if (typeof(data[[1]])!='double')
    {
        stop('Numeric type mismatch for column 1.\n TSDB type is INT64. Expecting input type DOUBLE.')
    }
    
    within(tsInfo,{
        for (i in c(2:numColsTS))
        {
            if (toupper(fieldTypes[i])=='INT8')
            {
                if (typeof(data[[i]])!='integer')
                {
                    stop(sprintf('\nNumeric type mismatch for column %d.\nTSDB type is INT8.  Expecting input type INTEGER.',i))
                }
            }
            else if (toupper(fieldTypes[i])=='INT32')
            {
                if (typeof(data[[i]])!='integer')
                {
                    stop(sprintf('\nNumeric type mismatch for column %d.\nTSDB type is INT32.  Expecting input type INTEGER.',i))
                }
            }
            else
            {
                if (typeof(data[[i]])!='double')
                {
                    stop(sprintf('\nNumeric type mismatch for column %d.\nTSDB type is DOUBLE.  Expecting input type DOUBLE.',i))
                }
            }
        }
    })
    
    #Checks complete.  Appending.
    .Call('TSDB_append',groupID,seriesName,tsInfo,data)    
       
    return(0)
}
    


    


