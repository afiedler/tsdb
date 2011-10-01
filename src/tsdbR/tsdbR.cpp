#include "tsdbR.h"

/**
<summary> Wraps H5open(). </summary>
<remarks> (From the online documentation) When the HDF5 Library is employed in a C application, this function
is normally called automatically, but if you find that an HDF5 library function
is failing inexplicably, try calling this function first. If you wish to elimnate
this possibility, it is safe to routinely call H5open before an application starts
working with the library as there are no damaging side-effects in calling it more
than once. </remarks>
<param name="status">Returns 1 if successful</param>
<returns> Returns 1 if successful. </returns>
*/
SEXP TSDBopen() {
try {
	H5open();
	return Rcpp::wrap(1);
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

/**
<summary> Wraps H5close(). </summary>
<remarks> (From the online documentation) H5close flushes all data to disk, closes all open HDF5 identifiers,
and cleans up all memory used by the HDF5 library. This function is generally
called when the application calls exit(), but may be called earlier in the
event of an emergency shutdown or out of a desire to free all resources used
by the HDF5 library. </remarks>
<returns> Returns 1 if successful.</returns>
*/
SEXP TSDBclose() {
try {
	H5close();
	return Rcpp::wrap(1);
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

/**
<summary> Wraps H5Fopen(). </summary>
<remarks> (From the online documentation) H5Fopen is the primary function
for accessing existing HDF5 files. This function opens the named file in
the specified access mode and with the specified access property list. </remarks>
<param name="fileName">The name of the file to be opened.</param>
<param name="permission">A string argument, either 'r' for reading,
or 'rw' for reading/writing.</param>
<returns> Returns a non-negative integer HDF5 group identifier if successful; otherwise
returns a negative value. </returns>
*/
SEXP TSDBopen_file(SEXP _filename, SEXP _permission) {
try {
	using namespace std;

	//checking arguments
	if (TYPEOF(_filename) != STRSXP || TYPEOF(_permission) != STRSXP)
		throw std::runtime_error("Expecting string arguments.");

	string filename = Rcpp::as<std::string>(_filename);
	string permission = Rcpp::as<std::string>(_permission);

	if (permission == "r") //read only
	{
		return Rcpp::wrap(H5Fopen(filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT));
	}
	else if (permission == "rw") //read and write for appending
	{
		return Rcpp::wrap(H5Fopen(filename.c_str(), H5F_ACC_RDWR, H5P_DEFAULT));
	}
	else
		throw std::runtime_error("Valid permissions are 'r' for read, and 'rw' for read/write.");
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

/**
<summary> Wraps H5Fclose(). </summary>
<remarks> (From the online documentation) H5Fclose terminates access to an HDF5 file by
flushing all data to storage and terminating access to the file through file_id.
If this is the last file identifier open for the file and no other access
identifier is open (e.g., a dataset identifier, group identifier, or
shared datatype identifier), the file will be fully closed and access
will end. </remarks>
<param name="groupID">Integer argument for the HDF5 file identifier (group ID).</param>
<returns> Returns a non-negative value if successful; otherwise
returns a negative value. </returns>
*/
SEXP TSDBclose_file(SEXP _groupID)
{
try{
	if (TYPEOF(_groupID) != INTSXP)
		throw std::runtime_error("Expecting an integer argument.");

	hid_t groupID = (hid_t) Rcpp::as<int>(_groupID);

	return Rcpp::wrap(H5Fclose(groupID));
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

/**
<summary> Lists all timeseries stored in a HDF5 file. Wraps H5Gget_info. </summary>
<param name="groupID">Integer argument for the HDF5 file identifier (group ID).</param>
<returns> Returns a list of available timeseries in the HDF5 file. </returns>
*/
SEXP TSDBtimeseries(SEXP _groupID)
{
try {
	using namespace std;

	//checking arguments
	if (TYPEOF(_groupID) != INTSXP)
		throw std::runtime_error("Expecting an integer argument.");

	hid_t groupID = (hid_t) Rcpp::as<int>(_groupID);

	herr_t status, size;
	H5G_info_t group_info;
	status = H5Gget_info(groupID, &group_info);

	if (status < 0)
		throw std::runtime_error("Could not retrieve information. Likely an invalid "
				"file indentifier (group ID).");

	if (group_info.nlinks == 0)
		throw std::runtime_error("No timeseries found.");

	char * name = NULL;
	Rcpp::StringVector tableNames(group_info.nlinks);

	for(hsize_t i = 0; i < group_info.nlinks; i++)
	{
		size = H5Lget_name_by_idx(groupID, ".", H5_INDEX_NAME, H5_ITER_INC, i, NULL, 0, H5P_DEFAULT);
		// allocate some space
		name = (char *) malloc(size+1);
		memset(name,0,size+1);
		status = H5Lget_name_by_idx(groupID, ".", H5_INDEX_NAME, H5_ITER_INC, i, name, size+1, H5P_DEFAULT);
		tableNames[i] = name;
		free(name);
	}
	return tableNames;
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

/**
<summary> Returns the properties of a particular timeseries in an
HDF5 file.</summary>
<param name="groupID">Integer argument for the HDF5 file identifier (group ID).</param>
<param name="seriesName">String argument for the timeseries name.</param>
<returns> Returns a list with the following named elements:
- recordCount - the number of records in the timeseries
- firstTimestamp - the time stamp of the first record
- firstTimestamp - the time stamp of the last record
- fields - a data frame containing the field name and field type</returns>
*/
SEXP TSDBget_properties(SEXP _groupID, SEXP _seriesName)
{
try {
	using namespace std;

	//checking arguments
	if (TYPEOF(_groupID) != INTSXP)
		throw std::runtime_error("First argument should an integer argument.");

	if (TYPEOF(_seriesName) != STRSXP)
		throw std::runtime_error("Second argument should a string.");

	hid_t groupID = (hid_t) Rcpp::as<int>(_groupID);
	string seriesName = Rcpp::as<std::string>(_seriesName);

	if (groupID < 0)
		throw std::runtime_error("Invalid group ID.");

	//creating timeseries object
	tsdb::Timeseries ts(groupID, seriesName);

	/***************************************
	RECORD COUNT
	***************************************/
	Rcpp::NumericVector recordCount(1);
	recordCount[0] = ts.getNRecords();

	/***************************************
	BEGINNING AND ENDING TIMESTAMPS
	***************************************/
	Rcpp::StringVector firstTimestamp(1), lastTimestamp(1);
	if (recordCount[0] > 0)
	{
		tsdb::RecordSet record = ts.recordSet((hsize_t) 0, (hsize_t) 0);
		firstTimestamp = record[0][0].toString();

		record = ts.recordSet(ts.getNRecords()-1,ts.getNRecords()-1);
		lastTimestamp = record[0][0].toString();
	}

	/***************************************
	COLUMN NAMES AND TYPES
	***************************************/
	size_t numFields = ts.structure()->getNFields();
	char** _fieldNames = ts.structure()->getNameOfFieldsAsArray();

	Rcpp::StringVector fieldNames(numFields);
	Rcpp::StringVector fieldTypes(numFields);

	for (size_t i=0; i<numFields; i++)
	{
		fieldNames[i] = _fieldNames[i];
		fieldTypes[i] = ts.structure()->getField(i)->getTSDBType();
	}

	//dataframe of field properties
	Rcpp::DataFrame fields = Rcpp::DataFrame::create(
		Rcpp::Named("name") = fieldNames,
		Rcpp::Named("type") = fieldTypes);

	return Rcpp::List::create(
			Rcpp::Named("recordCount")=recordCount,
			Rcpp::Named("firstTimestamp")=firstTimestamp,
			Rcpp::Named("lastTimestamp")=lastTimestamp,
			Rcpp::Named("fields")=fields);
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

/**
<summary> Pulls records from the timeseries.</summary>
<param name="groupID">Integer argument for the HDF5 file identifier (group ID).</param>
<param name="seriesName">String argument for the timeseries name.</param>
<param name="startTimestamp"> Double argument for the first wanted record. Since R doesn't
have a 64 bit integer, this argument will be received by the library as a double,
representing milliseconds since January 1, 1970. However, within R the argument can
be a string following the format '2009-04-01 16:30:00.003', which will be converted to
a double, or simply as a double. As long as the the argument respresents a time less
than 2^53 seconds from the epoch, which will happen in a few hundreds of thousands
of years, there should be no loss of precision.</param>
<param name="lastTimestamp"> Double arugment for the last timestamp of the wanted record.</param>
<param name="fieldsWanted"> Character vectora rgument for the wanted fields.</param>
<returns> Returns a data frame of wanted fields for the wanted records.</returns>
*/
SEXP TSDBget_records(SEXP _groupID, SEXP _seriesName,
		SEXP _startTimestamp, SEXP _endTimestamp, SEXP _fieldsWanted)
{
try {
	using namespace std;

	//checking arguments
	if (TYPEOF(_groupID) != INTSXP)
		throw std::runtime_error("Group ID should an integer argument.");

	if (TYPEOF(_seriesName) != STRSXP)
		throw std::runtime_error("Timeseries name should a string.");

	if (TYPEOF(_startTimestamp) != REALSXP || TYPEOF(_endTimestamp) != REALSXP)
		throw std::runtime_error("Timestamp arguments must have type double.");

	//fieldsWanted might be an empty argument
	if (TYPEOF(_fieldsWanted) != STRSXP && TYPEOF(_fieldsWanted) != NILSXP)
		throw std::runtime_error("Timestamp arguments must have type double.");

	//getting arguments
	hid_t groupID = (hid_t) Rcpp::as<int>(_groupID);
	string seriesName = Rcpp::as<std::string>(_seriesName);
	tsdb::timestamp_t startTimestamp = (tsdb::timestamp_t) Rcpp::as<double>(_startTimestamp);
	tsdb::timestamp_t endTimestamp = (tsdb::timestamp_t) Rcpp::as<double>(_endTimestamp);
	Rcpp::StringVector fieldsWanted;
	size_t numFieldsWanted;
	vector<size_t> indicesWanted;

	if (groupID < 0)
		throw std::runtime_error("Invalid group ID.");

	//creating timeseries object
	tsdb::Timeseries ts(groupID, seriesName);

	if (TYPEOF(_fieldsWanted) != NILSXP)
	{
		//figuring out the indices of wanted columns
		fieldsWanted = Rcpp::StringVector(_fieldsWanted);
		numFieldsWanted = fieldsWanted.length();
		for (size_t i=0; i<numFieldsWanted; i++)
		{
			size_t index = ts.structure()->
					getFieldIndexByName((char*)fieldsWanted[i]);
			indicesWanted.push_back(index);
		}
	}
	else
	{
		//grabbing all columns
		numFieldsWanted = ts.structure()->getNFields();
		char** fieldNames = ts.structure()->getNameOfFieldsAsArray();
		fieldsWanted = Rcpp::StringVector(numFieldsWanted);
		for (size_t i=0; i<numFieldsWanted; i++)
		{
			indicesWanted.push_back(i);
			fieldsWanted[i] = fieldNames[i];
		}
	}

	//loading the records into memory
	tsdb::RecordSet recordSet = ts.recordSet(startTimestamp, endTimestamp);
	size_t numRecords = recordSet.size();

	Rcpp::List records; //record container

	//looping through wanted columns
	for (size_t i = 0; i<numFieldsWanted; i++)
	{
		size_t index = indicesWanted[i];

		//type of the column
		string fieldType = ts.structure()->getField(index)->getTSDBType();

		if (fieldType == "Timestamp")
		{
			Rcpp::NumericVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toTimestamp();

			records.push_back(columnData,(char*)fieldsWanted[i]);
		}
		if (fieldType == "Date")
		{
			Rcpp::IntegerVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toDate();

			records.push_back(columnData,(char*)fieldsWanted[i]);
		}
		if (fieldType == "Int8")
		{
			Rcpp::IntegerVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toInt8();

			records.push_back(columnData,(char*)fieldsWanted[i]);
		}
		if (fieldType == "Int32")
		{
			Rcpp::IntegerVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toInt32();

			records.push_back(columnData,(char*)fieldsWanted[i]);
		}
		if (fieldType == "Double")
		{
			Rcpp::NumericVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toDouble();

			records.push_back(columnData,(char*)fieldsWanted[i]);
		}
		if (fieldType.find("String") != std::string::npos)
		{
			Rcpp::StringVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toString();

			records.push_back(columnData,(char*)fieldsWanted[i]);
		}
	}

	return Rcpp::DataFrame::create(records);
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

/**
<summary> Wraps H5Fcreate.</summary>
<param name="fileName">String argument of the file name.</param>
<param name="permission">Boolean argument for permission to overwite
an existing file. True allows for overwriting, and false by default.</param>
<returns> Returns a non-negative integer HDF5 group identifier if successful; otherwise
returns a negative value. </returns>
*/
SEXP TSDBcreate_file(SEXP _fileName, SEXP _permission)
{
try {
	using namespace std;
	//checking arguments
	if (TYPEOF(_fileName) != STRSXP)
		throw std::runtime_error("File name should be a string argument.");

	if (TYPEOF(_permission) != LGLSXP)
		throw std::runtime_error("Overwrite permission should be a logical argument.");

	string fileName = Rcpp::as<string>(_fileName);
	bool permission = Rcpp::as<bool>(_permission);

	if (permission == true) //allowed to overwrite an existing file
		return Rcpp::wrap(H5Fcreate(fileName.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,H5P_DEFAULT));
	else	//fails if file exists
		return Rcpp::wrap(H5Fcreate(fileName.c_str(),H5F_ACC_EXCL,H5P_DEFAULT,H5P_DEFAULT));
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

/**
<summary> Creates a new timeseries within an HDF5 file.</summary>
<param name="groupID">Integer argument for the HDF5 file identifier (group ID).</param>
<param name="seriesName">String argument for the timeseries name.</param>
<param name="seriesDescription">String argument for timeseries' description.</param>
<param name="fields">Data frame argument containing two named elements:
-fieldNames - names of the fields
-fieldTypes - types of the fields
These elements must have the same dimension.</param>
<returns> Returns a non-negative integer HDF5 group identifier if successful; otherwise
returns a negative value. </returns>
*/
SEXP TSDBcreate_timeseries(SEXP _groupID, SEXP _seriesName,
		SEXP _seriesDescription, SEXP _fields)
{
try {
	using namespace std;
	//checking arguments
	if (TYPEOF(_groupID) != INTSXP)
		throw std::runtime_error("Group ID should be an integer argument.");

	if (TYPEOF(_seriesName) != STRSXP)
		throw std::runtime_error("Timeseries name should be a string argument.");

	if (TYPEOF(_seriesDescription) != STRSXP)
		throw std::runtime_error("Timeseries description should be a string argument.");

	if (TYPEOF(_fields) != VECSXP)
		throw std::runtime_error("Fields argument should be a named list.");

	//checking properties of the list
	Rcpp::List fieldsList(_fields);
	Rcpp::StringVector listNames = fieldsList.attr("names");

	//checking for the existence of 'fieldNames' and 'fieldTypes' list elements
	int check = (int) std::count(listNames.begin(),listNames.end(),"fieldNames");
	if (check != 1)
		throw std::runtime_error("Fields argument should have one element named 'fieldNames'.");

	check = (int) std::count(listNames.begin(),listNames.end(),"fieldTypes");
	if (check != 1)
		throw std::runtime_error("Fields argument should have one element named 'fieldTypes'.");

	Rcpp::StringVector fieldNames = fieldsList["fieldNames"];
	Rcpp::StringVector fieldTypes = fieldsList["fieldTypes"];

	if (fieldNames.length() != fieldNames.length())
		throw std::runtime_error("'fieldNames' and 'fieldTypes' should have the same length.");

	//grabbing other arguments
	string seriesName = Rcpp::as<string>(_seriesName);
	string seriesDescription = Rcpp::as<string>(_seriesDescription);
	int groupID = Rcpp::as<int>(_groupID);

	if (groupID < 0)
		throw std::runtime_error("Invalid group ID.");

	//building structure of the new timeseries
	int numFields = fieldNames.length();
	vector<tsdb::Field*> fields;

	//first field is always the timestamp
	fields.push_back(new tsdb::TimestampField("TSDB_timestamp"));

	for (int i=0; i<numFields; i++)
	{
		string name = (string) fieldNames[i];
		string type = (string) fieldTypes[i];

		//transforming to lower case, so the comparison is case INsensitive
		boost::to_lower(type);
		if (type == "int8")
			fields.push_back(new tsdb::Int8Field(name));
		else if (type == "int32")
			fields.push_back(new tsdb::Int32Field(name));
		else if (type == "double")
			fields.push_back(new tsdb::DoubleField(name));
		else if (type == "date")
			fields.push_back(new tsdb::DateField(name));
		else if (type.find("string") != std::string::npos)
		{
			char* stringLengthPr = strtok((char*) type.c_str(),"(");
			if (stringLengthPr == NULL)
				throw std::runtime_error("Field type for a string must have the form 'string(n)', for some integer n.");

			stringLengthPr = strtok(NULL, ")");
			if (stringLengthPr == NULL)
				throw std::runtime_error("Field type for a string must have the form 'string(n)', for some integer n.");

			int stringLength = atoi(stringLengthPr);
			fields.push_back(new tsdb::StringField(name,stringLength));
		}
	}

	boost::shared_ptr<tsdb::Structure> st =
			boost::make_shared<tsdb::Structure>(fields,false);
	tsdb::Timeseries ts =
			tsdb::Timeseries(groupID,seriesName,seriesDescription,st);

	Rcpp::IntegerVector status(1);
	status[0] = 1;
	return status;
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

std::string TSDBappendErrorMessage(std::string fieldName,
		std::string TSDBtype)
{
	return "Field " + fieldName	+ " is type " + TSDBtype +
		" in the TSDB table. The field in the appending data "
		"has the incorrect type.";
}

/**
<summary> Appends data onto an existing timeseries.</summary>
<remarks> There are several checks which will be performed. All must pass.
- appropriate data types between the timeseries field and the appending data
- all fields of the timeseries must exist in the appending data
- all fields of the data frame must have the same length</remarks>

<param name="groupID">Integer argument for the HDF5 file identifier (group ID).</param>
<param name="seriesName">String argument for the timeseries name.</param>
<param name="appendData">Data frame argument of the data to append.</param>
<param name="discardOverlap">If true, appended records that overlap with
existing records are discarded. </param>
<returns> Returns 1 if successful.</returns>
*/
SEXP TSDBappend(SEXP _groupID, SEXP _seriesName, SEXP _appendData,
				SEXP _discardOverlap)
{
try {
	using namespace std;
	//checking arguments
	if (TYPEOF(_groupID) != INTSXP)
		throw std::runtime_error("Group ID should be an integer argument.");

	if (TYPEOF(_seriesName) != STRSXP)
		throw std::runtime_error("Timeseries name should be a string argument.");

	if (TYPEOF(_appendData) != VECSXP)
		throw std::runtime_error("Data should be a data frame.");

	/******************************************************
	CHECKING PROPERTIES OF THE TABLE AND THE APPENDING DATA
	*******************************************************/
	//properties of the table
	string seriesName = Rcpp::as<string>(_seriesName);
	int groupID = Rcpp::as<int>(_groupID);
	tsdb::Timeseries ts(groupID, seriesName);
	size_t tableFieldCount = ts.structure()->getNFields();

	//properties of the appending data frame
	Rcpp::List appendData(_appendData);
	Rcpp::StringVector appendDataNames = appendData.attr("names");
	size_t appendingFieldCount = appendDataNames.length();

	//checking for number of fields
	if (appendingFieldCount != tableFieldCount)
		throw std::runtime_error("Appending data frame and the TSDB table "
				"have a different number of fields.");

	//checking to see if the field in the appending data exists in the
	//TSDB table; also checking for types
	int numRecordsToAppend = LENGTH(appendData[(string) appendDataNames[0]]);
	for (size_t i=0; i<appendingFieldCount; i++)
	{
		//if the index isn't found, an exception will occur
		size_t index = ts.structure()->getFieldIndexByName((string) appendDataNames[i]);

		//checking the types match
		string TSDBtype = ts.structure()->getField(index)->getTSDBType();
		if (TSDBtype == "Timestamp" || TSDBtype == "Double")
		{
			if (TYPEOF(appendData[(string) appendDataNames[i]]) != REALSXP)
			{
				throw std::runtime_error(
				    TSDBappendErrorMessage((string) appendDataNames[i],TSDBtype));
			}
		}
		else if (TSDBtype == "Int8" || TSDBtype == "Int32" || TSDBtype == "Date")
		{
		    if (TYPEOF(appendData[(string) appendDataNames[i]]) != INTSXP)
			{
				throw std::runtime_error(
				    TSDBappendErrorMessage((string) appendDataNames[i],TSDBtype));
			}
		}
		else if (TSDBtype == "String")
		{
		    if (TYPEOF(appendData[(string) appendDataNames[i]]) != STRSXP)
			{
				throw std::runtime_error(
				    TSDBappendErrorMessage((string) appendDataNames[i],TSDBtype));
			}
		}

		//checking existence of table fields in the data frame
		string fieldName = ts.structure()->getField(i)->getName();
		int check = (int) std::count(appendDataNames.begin(),appendDataNames.end(),fieldName.c_str());
		if (check != 1)
		{
			if (check == 0)
				throw std::runtime_error("Field " + fieldName + " was not found "
						"in the appending data.");
			else
				throw std::runtime_error("Field " + fieldName + " was found "
						"multiple times.");
		}

		//checking for uniform length in data frame.
		//this could be an issue if a named list is passed as an argument.
		if (numRecordsToAppend != LENGTH(appendData[(string) appendDataNames[0]]))
			throw std::runtime_error("All fields in the appending data must have"
					" the same length/");
	}

	/**************************************
	CHECKS PASSED. APPENDING DATA.
	**************************************/
	//vector of formatted input, ready for appending
	/*
	http://stackoverflow.com/questions/7251253/c-no-matching-function-for-call-but-the-candidate-has-the-exact-same-signatur
	explains why 'tsdb::RecordSet records((size_t) numRecordsToAppend, ts.structure());'
	would not work.
	*/
	boost::shared_ptr<tsdb::Structure> tsStructure = ts.structure();
	tsdb::RecordSet records((size_t) numRecordsToAppend, tsStructure);

	for (size_t dfIndex=0; dfIndex<appendingFieldCount; dfIndex++)
	{
		string dfName = (string) appendDataNames[dfIndex];

		//index in the TSDB table
		size_t tableIndex = ts.structure()->getFieldIndexByName(dfName);
		string TSDBtype = ts.structure()->getField(tableIndex)->getTSDBType();

		if (TSDBtype == "Timestamp")
		{
			SEXP dfColumn = VECTOR_ELT(_appendData,dfIndex);

			for (int row=0; row<numRecordsToAppend; row++)
				records[row][tableIndex] = (tsdb::timestamp_t) (REAL(dfColumn)[row]);
		}
		else if (TSDBtype == "Double")
		{
			SEXP dfColumn = VECTOR_ELT(_appendData,dfIndex);

			for (int row=0; row<numRecordsToAppend; row++)
				records[row][tableIndex] = (tsdb::ieee64_t) (REAL(dfColumn)[row]);
		}
		else if (TSDBtype == "Int8")
		{
			SEXP dfColumn = VECTOR_ELT(_appendData,dfIndex);

			for (int row=0; row<numRecordsToAppend; row++)
				records[row][tableIndex] = (tsdb::int8_t) (INTEGER(dfColumn)[row]);
		}
		else if (TSDBtype == "Int32")
		{
			SEXP dfColumn = VECTOR_ELT(_appendData,dfIndex);

			for (int row=0; row<numRecordsToAppend; row++)
				records[row][tableIndex] = (tsdb::int32_t) (INTEGER(dfColumn)[row]);
		}
		else if (TSDBtype == "Date")
		{
			SEXP dfColumn = VECTOR_ELT(_appendData,dfIndex);

			for (int row=0; row<numRecordsToAppend; row++)
				records[row][tableIndex] = (tsdb::date_t) (INTEGER(dfColumn)[row]);
		}
		else if (TSDBtype.find("String") != std::string::npos)
		{
			Rcpp::StringVector dfColumn(VECTOR_ELT(_appendData,dfIndex));

			for (int row=0; row<numRecordsToAppend; row++)
				records[row][tableIndex] = (string) dfColumn[row];
		}
	}

	//appending the data
	bool discardOverlap = Rcpp::as<bool>(_discardOverlap);
	ts.appendRecordSet(records,discardOverlap);

	return Rcpp::wrap(1);
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}
