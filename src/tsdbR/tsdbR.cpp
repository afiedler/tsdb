#include "tsdbR.h"

SEXP TSDBopen() {
try {
	H5open();
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

SEXP TSDBclose() {
try {
	H5close();
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

SEXP TSDBopen_file(SEXP _filename, SEXP _permission) {
try {
	using namespace std;

	//checking arguments
	if (TYPEOF(_filename) != STRSXP || TYPEOF(_permission) != STRSXP)
		throw std::runtime_error("Expecting string arguments.");

	string filename = Rcpp::as<std::string>(_filename);
	string permission = Rcpp::as<std::string>(_permission);

	Rcpp::IntegerVector groupID(1);

	if (permission.compare("r") == 0) //read only
	{
		groupID[0] = H5Fopen(filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	}
	else if (permission.compare("rw") == 0) //read and write for appending
	{
		groupID[0] = H5Fopen(filename.c_str(), H5F_ACC_RDWR, H5P_DEFAULT);
	}
	else
		throw std::runtime_error("Valid permissions are 'r' for read, and 'rw' for read/write.");

	return groupID;
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue; // -Wall
}

SEXP TSDBclose_file(SEXP _groupID)
{
try{
	if (TYPEOF(_groupID) != INTSXP)
		throw std::runtime_error("Expecting an integer argument.");

	hid_t groupID = (hid_t) Rcpp::as<int>(_groupID);

	Rcpp::IntegerVector status(1);
	status[0] = (int) H5Fclose(groupID);

	return status;
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}


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

SEXP TSDBget_properties(SEXP _groupID, SEXP _timeseriesName)
{
try {
	using namespace std;

	//checking arguments
	if (TYPEOF(_groupID) != INTSXP)
		throw std::runtime_error("First argument should an integer argument.");

	if (TYPEOF(_timeseriesName) != STRSXP)
		throw std::runtime_error("Second argument should a string.");

	hid_t groupID = (hid_t) Rcpp::as<int>(_groupID);
	string timeseriesName = Rcpp::as<std::string>(_timeseriesName);

	if (groupID < 0)
		throw std::runtime_error("Invalid group ID.");

	//creating timeseries object
	tsdb::Timeseries ts(groupID, timeseriesName);

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
	//building a named list
	Rcpp::List columnProperties = Rcpp::List::create(
			Rcpp::Named("name") = fieldNames,
			Rcpp::Named("type") = fieldTypes);

	//feading list to dataframe
	Rcpp::DataFrame test = Rcpp::DataFrame::create(columnProperties);

	return Rcpp::List::create(
			Rcpp::Named("count")=recordCount,
			Rcpp::Named("firstTimestamp")=firstTimestamp,
			Rcpp::Named("lastTimestamp")=lastTimestamp,
			Rcpp::Named("columns")=test);
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

SEXP TSDBget_records(SEXP _groupID, SEXP _timeseriesName,
		SEXP _startTimestamp, SEXP _endTimestamp, SEXP _columnsWanted)
{
try {
	using namespace std;

	//checking arguments
	if (TYPEOF(_groupID) != INTSXP)
		throw std::runtime_error("Group ID should an integer argument.");

	if (TYPEOF(_timeseriesName) != STRSXP)
		throw std::runtime_error("Timeseries name should a string.");

	if (TYPEOF(_startTimestamp) != REALSXP || TYPEOF(_endTimestamp) != REALSXP)
		throw std::runtime_error("Timestamp arguments must have type double.");

	//columnsWanted might be an empty argument
	if (TYPEOF(_columnsWanted) != STRSXP && TYPEOF(_columnsWanted) != NILSXP)
		throw std::runtime_error("Timestamp arguments must have type double.");

	//getting arguments
	hid_t groupID = (hid_t) Rcpp::as<int>(_groupID);
	string timeseriesName = Rcpp::as<std::string>(_timeseriesName);
	tsdb::timestamp_t startTimestamp = (tsdb::timestamp_t) Rcpp::as<double>(_startTimestamp);
	tsdb::timestamp_t endTimestamp = (tsdb::timestamp_t) Rcpp::as<double>(_endTimestamp);
	Rcpp::StringVector columnsWanted;
	size_t numColumnsWanted;
	vector<size_t> indicesWanted;

	if (groupID < 0)
		throw std::runtime_error("Invalid group ID.");

	//creating timeseries object
	tsdb::Timeseries ts(groupID, timeseriesName);

	if (TYPEOF(_columnsWanted) != NILSXP)
	{
		//figuring out the indices of wanted columns
		columnsWanted = Rcpp::StringVector(_columnsWanted);
		numColumnsWanted = columnsWanted.length();
		for (size_t i=0; i<numColumnsWanted; i++)
		{
			size_t index = ts.structure()->
					getFieldIndexByName((char*)columnsWanted[i]);
			indicesWanted.push_back(index);
		}
	}
	else
	{
		//grabbing all columns
		numColumnsWanted = ts.structure()->getNFields();
		char** fieldNames = ts.structure()->getNameOfFieldsAsArray();
		for (size_t i=0; i<numColumnsWanted; i++)
		{
			indicesWanted.push_back(i);
			columnsWanted[i] = fieldNames[i];
		}
	}

	//loading the records into memory
	tsdb::RecordSet recordSet = ts.recordSet(startTimestamp, endTimestamp);
	size_t numRecords = recordSet.size();

	Rcpp::List records; //record container

	//looping through wanted columns
	for (size_t i = 0; i<numColumnsWanted; i++)
	{
		size_t index = indicesWanted[i];

		//type of the column
		string fieldType = ts.structure()->getField(index)->getTSDBType();

		if (fieldType == "Timestamp")
		{
			Rcpp::NumericVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toTimestamp();

			records.push_back(columnData,(char*)columnsWanted[i]);
		}
		if (fieldType == "Date")
		{
			Rcpp::IntegerVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toDate();

			records.push_back(columnData,(char*)columnsWanted[i]);
		}
		if (fieldType == "Int8")
		{
			Rcpp::IntegerVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toInt8();

			records.push_back(columnData,(char*)columnsWanted[i]);
		}
		if (fieldType == "Int32")
		{
			Rcpp::IntegerVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toInt32();

			records.push_back(columnData,(char*)columnsWanted[i]);
		}
		if (fieldType == "Double")
		{
			Rcpp::NumericVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toDouble();

			records.push_back(columnData,(char*)columnsWanted[i]);
		}
		if (fieldType == "String")
		{
			Rcpp::StringVector columnData(numRecords);

			for (size_t row=0; row<numRecords; row++)
				columnData[row] = recordSet[row][index].toString();

			records.push_back(columnData,(char*)columnsWanted[i]);
		}
	}

	return records;
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

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
	Rcpp::IntegerVector groupID(1);

	if (permission == true) //allowed to overwrite an existing file
		groupID[0] = H5Fcreate(fileName.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,H5P_DEFAULT);
	else	//fails if file exists
		groupID[0] = H5Fcreate(fileName.c_str(),H5F_ACC_EXCL,H5P_DEFAULT,H5P_DEFAULT);

	return groupID;
}
catch( std::exception &ex ) {
	forward_exception_to_r(ex);
} catch(...) {
	::Rf_error( "c++ exception (unknown reason)" );
}
return R_NilValue;
}

SEXP TSDBcreate_timeseries(SEXP _groupID, SEXP _timeseriesName,
		SEXP _timeseriesDescription, SEXP _fields)
{
try {
	using namespace std;
	//checking arguments
	if (TYPEOF(_groupID) != INTSXP)
		throw std::runtime_error("Group ID should be an integer argument.");

	if (TYPEOF(_timeseriesName) != STRSXP)
		throw std::runtime_error("Timeseries name should be a string argument.");

	if (TYPEOF(_timeseriesDescription) != STRSXP)
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
	string timeseriesName = Rcpp::as<string>(_timeseriesName);
	string timeseriesDescription = Rcpp::as<string>(_timeseriesDescription);
	int groupID = Rcpp::as<int>(_groupID);

	if (groupID < 0)
		throw std::runtime_error("Invalid group ID.");

	//building structure of the new timeseries
	int numFields = fieldNames.length();
	vector<tsdb::Field*> fields;

	//first field is always the timestamp
	fields.push_back(new tsdb::TimestampField("_TSDB_timestamp"));

	for (int i=0; i<numFields; i++)
	{
		string name = (char*) fieldNames[i];
		string type = (char*) fieldTypes[i];

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
			tsdb::Timeseries(groupID,timeseriesName,timeseriesDescription,st);

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
