/* NOTE: mex.h must be included before tsdbmatlab.h */
#include "mex.h"
#include "hdf5.h"
#include "structure.h"
#include "timeseries.h"
#include "tsdb.h"
#include "tsdbmatlab.h"
#include "cell.h"
#include "recordset.h"
#include "record.h"
#include <string>
#include <time.h>

#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>


void TSDBopen(void) {
	H5open();
}

void TSDBclose(void) {
	H5close();
}

/** <summary>Opens a TSDB file for read or read-write access</summary>
 * <param name="filename">The file to open</param>
 * <param name="permission">"r" for read only, "rw" for read-write</permission>
 */
hid_t TSDBopen_file(const char * filename, const char * permission) {
	if (!strcmp(permission,"r")) //read only
	{
		return H5Fopen(filename, H5F_ACC_RDONLY, H5P_DEFAULT);
	}
	else if (!strcmp(permission,"rw")) //read and write for appending
	{
		return H5Fopen(filename, H5F_ACC_RDWR, H5P_DEFAULT);
	}
	mexErrMsgTxt("Not a valid permission.  Must be 'r' or 'rw'.");
	return -1;
}

/** <summary>Creates a new TSDB file</summary>
 * <param name="filename">File to create</param>
 */
int TSDBcreate_file(const char * filename) {
	try {
		hid_t out_fid = H5Fcreate(filename,
			H5F_ACC_EXCL,H5P_DEFAULT,H5P_DEFAULT);

		if (out_fid==-1) {
			mexErrMsgTxt("File already exists!");
		}
		return out_fid;
	}	
	catch(std::exception &e) {
		std::string errormsg = std::string("Error in C code: ") + e.what();
		mexErrMsgTxt(errormsg.c_str());
	} 
}

/** <summary>Creates a timeseries in a already-open file</summary>
 * <param name="fid">File id</param>
 * <param name="seriesname">Name of the series</param>
 * <param name="desc">Series description</param>
 * <param name="columnTypes">Array of <c>char*</c> strings, with column types</param>
 * <param name="columnNames">Array of <c>char*</c> strings, with column names</param>
 * <param name="numColumns">Number of columns</param>
 */
void TSDBcreate_timeseries(int fid, const char * seriesname, const char * desc,
					const char * columnTypes[], const char * columnNames[],int numColumns) {
	try {
		std::vector<tsdb::Field*> fields;

		/* Add the timestamp field */
		fields.push_back(new tsdb::TimestampField("_TSDB_timestamp"));

		for (int i=0; i<numColumns; i++)
		{
			std::string type = std::string(columnTypes[i]);
			//capitalizing type
			std::transform(type.begin(),type.end(),type.begin(), std::toupper);

			std::string name  = std::string(columnNames[i]);

			if (type.find("STRING(") != std::string::npos) {
				//getting the string size
				std::string size_str = type.substr(7,type.length()-8);
				int size = atoi(size_str.c_str());
				fields.push_back(new tsdb::StringField(name,size));
			}
			//else if (type == "DATE") {
			//	fields.push_back(new tsdb::date_t(type));
			//}
			else if (type == "INT8") {
				fields.push_back(new tsdb::Int8Field(name));
			}
			else if (type == "INT32") {
				fields.push_back(new tsdb::Int32Field(name));
			}
			else if (type == "DOUBLE") {
				fields.push_back(new tsdb::DoubleField(name));
			}
		}
		//creating the series
		boost::shared_ptr<tsdb::Structure> st = boost::make_shared<tsdb::Structure>(fields,false); /* Note: no memory alignment for better space utilization */
		tsdb::Timeseries ts = tsdb::Timeseries(fid,seriesname,"",st);
		//tsdb::Timeseries out(fid,seriesname,"testing",fields);
	}	
	catch(std::exception &e) {
		std::string errormsg = std::string("Error in C code: ") + e.what();
		mexErrMsgTxt(errormsg.c_str());
	} 
}


int TSDBclose_file(int fid) {
	hid_t status = H5Fclose(fid);
	return status;
}

mxArray* TSDBread_timeseries_by_timestamp(int loc_id, const char * series, long long start_ts, long long end_ts) {

	try {		
		// Open the timeseries
		tsdb::Timeseries ts = tsdb::Timeseries(loc_id, std::string(series));

		//how many columns are there?
		size_t numFields = ts.structure()->getNFields();

		//getting the names of the columns
		char** fieldNames = ts.structure()->getNameOfFieldsAsArray();

		//dimensions of the output structure
		mwSize dims[2];
		dims[0]=1;
		dims[1]=1;

		// Here, we need to convert leading underscores to the letter "u", since MATLAB
		// variables must start with a letter
		for(size_t i=0;i<numFields;i++) {
			if(fieldNames[i][0] == '_')
				fieldNames[i][0] = 'u';
		}


		//forming the matlab structure
		mxArray* recordStructure = mxCreateStructArray(2,dims,numFields,(const char**)fieldNames);

		//loading the records
		tsdb::RecordSet recSet = ts.recordSet(
			(tsdb::timestamp_t) start_ts, 
			(tsdb::timestamp_t) end_ts);

		//how many records to be returned
		size_t numRecords = recSet.size();

		for(size_t i=0;i<numFields;i++) 
		{
			std::string fieldType = ts.structure()->getField(i)->getTSDBType();

			if (!fieldType.compare("Timestamp"))
			{
				//mxArray to be placed into the record structure
				mxArray* structureElement = mxCreateNumericMatrix(numRecords,1,mxUINT64_CLASS,mxREAL);

				if (structureElement==NULL) 
					throw std::runtime_error("too many records to fetch at once");

				//ptr to set values of mxarray
				tsdb::timestamp_t* dataPtr = (tsdb::timestamp_t*) mxGetData(structureElement);

				//setting values to mxarray
				for (size_t j=0;j<numRecords;j++)
					dataPtr[j] = recSet[j][i].toTimestamp();
				
				//setting mxarray into the record structure
				mxSetFieldByNumber(recordStructure,0,i,structureElement);
			}
			else if (!fieldType.compare("Date"))
			{
				//mxArray to be placed into the record structure
				mxArray* structureElement = mxCreateNumericMatrix(numRecords,1,mxINT32_CLASS,mxREAL);

				if (structureElement==NULL) 
					throw std::runtime_error("too many records to fetch at once");

				//ptr to set values of mxarray
				tsdb::date_t* dataPtr = (tsdb::date_t*) mxGetData(structureElement);
				
				//setting values to mxarray
				for (size_t j=0;j<numRecords;j++)
					dataPtr[j] = recSet[j][i].toDate();	
				
				//setting mxarray into the record structure
				mxSetFieldByNumber(recordStructure,0,i,structureElement);
			}
			else if (!fieldType.compare("Int8"))
			{
				mxArray* structureElement = mxCreateNumericMatrix(numRecords,1,mxINT8_CLASS,mxREAL);
				
				if (structureElement==NULL) 
					throw std::runtime_error("too many records to fetch at once");
				
				//ptr to set values of mxarray
				tsdb::int8_t* dataPtr = (tsdb::int8_t* ) mxGetData(structureElement);
				
				//setting values to mxarray
				for (size_t j=0;j<numRecords;j++)
					dataPtr[j] = recSet[j][i].toInt8();	
				
				//setting mxarray into the record structure
				mxSetFieldByNumber(recordStructure,0,i,structureElement);
			}
			else if (!fieldType.compare("Int32"))
			{
				mxArray* structureElement = mxCreateNumericMatrix(numRecords,1,mxINT32_CLASS,mxREAL);
				
				if (structureElement==NULL) 
					throw std::runtime_error("too many records to fetch at once");
				
				//ptr to set values of mxarray
				tsdb::int32_t* dataPtr = (tsdb::int32_t*) mxGetData(structureElement);
				
				//setting values to mxarray
				for (size_t j=0;j<numRecords;j++)
					dataPtr[j] = recSet[j][i].toInt32();	
				
				//setting mxarray into the record structure
				mxSetFieldByNumber(recordStructure,0,i,structureElement);
			}
			else if (!fieldType.compare("Double"))
			{
				mxArray* structureElement = mxCreateNumericMatrix(numRecords,1,mxDOUBLE_CLASS,mxREAL);
				
				if (structureElement==NULL) 
					throw std::runtime_error("too many records to fetch at once");
				
				//ptr to set values of mxarray
				tsdb::ieee64_t* dataPtr = (tsdb::ieee64_t*) mxGetData(structureElement);
				
				//setting values to mxarray
				for (size_t j=0;j<numRecords;j++)
					dataPtr[j] = recSet[j][i].toDouble();	

				//setting mxarray into the record structure
				mxSetFieldByNumber(recordStructure,0,i,structureElement);
			}
			//else if (fieldType.find("String") != std::string::npos)
			else if (!fieldType.compare(0,6,"String"))
			{
				mwSize cellDims[2];
				cellDims[0] = numRecords;
				cellDims[1] = 1;
				mxArray* structureElement = mxCreateCellArray(2,cellDims);

				if (structureElement==NULL) 
					throw std::runtime_error("too many records to fetch at once");

				//setting values to mxarray
				for (size_t j=0;j<numRecords;j++)
					mxSetCell(structureElement,j,mxCreateString(recSet[j][i].toString().c_str()));

				//setting mxarray into the record structure
				mxSetFieldByNumber(recordStructure,0,i,structureElement);
			}

		}

		return recordStructure;
	} catch(std::exception &e) {
		std::string errormsg = std::string("Error in C code: ") + e.what();
		mexErrMsgTxt(errormsg.c_str());
		return mxCreateCellMatrix(0,0);
	}
}

mxArray* TSDBget_timeseries_info(int loc_id, const char * series) {
	try {		
		//dimensions of the output structure
		mwSize dims[2];
		dims[0]=1;
		dims[1]=1;

		//names of the structure elements
		const char* structureNames[] = 
			{"numRecords",
			"startTimeStamp",
			"endTimeStamp",
			"fieldNames",
			"fieldTypes"};

		//the structure pointer
		mxArray* infoStructure = mxCreateStructArray(2,dims,5,structureNames);
		mxArray* structureElement;

		// Open the timeseries
		tsdb::Timeseries ts = tsdb::Timeseries(loc_id, std::string(series));

		//number of records
		structureElement = mxCreateNumericMatrix(1,1,mxUINT64_CLASS,mxREAL);
		//setting value into mxArray
		*((hsize_t*) mxGetData(structureElement)) = ts.getNRecords();
		//setting mxArray into structure
		mxSetFieldByNumber(infoStructure,0,0,structureElement);

		//beginning/ending timestamps
		if (ts.getNRecords())
		{
			//beginning timestamp
			void * record = NULL;
			record = ts.getRecordsById(0,0);
			structureElement  = mxCreateString(
				ts.structure()->getField(0)->toString(record).c_str()
				);
			mxSetFieldByNumber(infoStructure,0,1,structureElement);
			//ending timestamp
			record = ts.getLastRecord();
			structureElement  = mxCreateString(
				ts.structure()->getField(0)->toString(record).c_str()
				);			
			mxSetFieldByNumber(infoStructure,0,2,structureElement);
		}

		//fieldNames
		size_t numFields = ts.structure()->getNFields();
		char** fieldNames = ts.structure()->getNameOfFieldsAsArray();
		dims[0] = 1;
		dims[1] = numFields;
		//setting field names into mxArray cell array
		structureElement = mxCreateCellArray(2,dims);
		mxSetCell(structureElement,0,mxCreateString("TSDB_timestamp"));
		for (size_t i=1;i<numFields;i++)
			mxSetCell(structureElement,i,mxCreateString(fieldNames[i]));
		//setting cell array into the output structure
		mxSetFieldByNumber(infoStructure,0,3,structureElement);

		//fieldTypes
		structureElement = mxCreateCellArray(2,dims);
		for (size_t i=0;i<numFields;i++)
		{
			//setting field names into mxArray cell array
			mxSetCell(structureElement,i,
				mxCreateString(ts.structure()->getField(i)->getTSDBType().c_str())
				);
		}
		//setting cell array into the output structure
		mxSetFieldByNumber(infoStructure,0,4,structureElement);

		return infoStructure;
	} catch(std::exception &e) {
		std::string errormsg = std::string("Error in C code: ") + e.what();
		mexErrMsgTxt(errormsg.c_str());
		return mxCreateCellMatrix(0,0);
	}
}

mxArray* TSDBget_timeseries_names(int fid)
{
	try {
		//code grabbed from TSDB GUI
		herr_t status;
		herr_t size;
		H5G_info_t group_info;
		status = H5Gget_info(fid, &group_info);
		char * name = NULL;

		mxArray* cells = mxCreateCellMatrix(1, (mwSize) group_info.nlinks);

		for(hsize_t i = 0; i< group_info.nlinks; i++) {
			size = H5Lget_name_by_idx(fid, ".", H5_INDEX_NAME, H5_ITER_INC, i, NULL, 0, H5P_DEFAULT);
			// allocate some space
			name = (char *) malloc(size+1);
			memset(name,0,size+1);
			status = H5Lget_name_by_idx(fid, ".", H5_INDEX_NAME, H5_ITER_INC, i, name, size+1, H5P_DEFAULT);
			mxSetCell(cells,(mwIndex) i,mxCreateString(name));
			free(name);
		}

		return cells;
	} catch(std::exception &e) {
		std::string errormsg = std::string("Error in C code: ") + e.what();
		mexErrMsgTxt(errormsg.c_str());
		return mxCreateCellMatrix(0,0);
	}
}

int TSDBtimeseries_append(int loc_id, const char * series,mxArray * data)
{
	try {
		tsdb::Timeseries ts(loc_id, std::string(series));

		int inNumColumns = mxGetN(data);

		//checking to see variable types match
		//using boost algorithm to avoid case sensitive comparisons
		for (int i=0;i<inNumColumns;i++)
		{
			mxArray * cellElement = mxGetCell(data,i);
			std::string matlabType = std::string(mxGetClassName(cellElement));
			//matlab int64 needs translating to "Timestamp"
			if (!matlabType.compare("int64")) matlabType = "Timestamp"; 
			std::string tsType = ts.structure()->getField(i)->getTSDBType();
			//comparison of field types
			if(!boost::algorithm::iequals(matlabType,tsType))
			{
				std::string errormsg = std::string("Variable types don't match!");
				mexErrMsgTxt(errormsg.c_str());
			}
		}	

		//data might be a row or column vector
		int inNumRows = mxGetM(mxGetCell(data,0))<mxGetN(mxGetCell(data,0))?
			mxGetN(mxGetCell(data,0)):mxGetM(mxGetCell(data,0));

		//vector of formatted input, ready for appending
		tsdb::RecordSet records(inNumRows,ts.structure());

		//populating vector
		for (int column=0;column<inNumColumns;column++)
		{
			mxArray * cellElement = mxGetCell(data,column);
			std::string tsType = ts.structure()->getField(column)->getTSDBType();

			if (boost::algorithm::iequals(tsType,"Timestamp")) {
				tsdb::timestamp_t * values = (tsdb::timestamp_t *) mxGetData(cellElement);
				for (int row=0;row<inNumRows;row++)	records[row][column] = values[row];
			}
			else if (boost::algorithm::iequals(tsType,"INT8")) {
				tsdb::int8_t * values = (tsdb::int8_t *) mxGetData(cellElement);
				for (int row=0;row<inNumRows;row++)	records[row][column] = values[row];
			}
			else if (boost::algorithm::iequals(tsType,"INT32")) {
				tsdb::int32_t * values = (tsdb::int32_t *) mxGetData(cellElement);
				for (int row=0;row<inNumRows;row++)	records[row][column] = values[row];
			}
			else if (boost::algorithm::iequals(tsType,"DOUBLE")) {
				tsdb::ieee64_t * values = (tsdb::ieee64_t *) mxGetData(cellElement);
				for (int row=0;row<inNumRows;row++)	records[row][column] = values[row];
			}
		}

		//appending the data
		ts.appendRecordSet(records,false);

		return 1;
	}	catch(std::exception &e) {
		std::string errormsg = std::string("Error in C code: ") + e.what();
		mexErrMsgTxt(errormsg.c_str());
		return 0;
	}
}

