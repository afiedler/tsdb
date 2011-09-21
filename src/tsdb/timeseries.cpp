/* STL Includes */
#include <string>
#include <vector>
#include <stddef.h>
#include "limits.h"
#include <boost/make_shared.hpp>

/* HDF5 Includes */
#include "hdf5.h"
#include "hdf5_hl.h"

/* TSDB Includes */
#include "tsdb.h"
#include "timeseries.h"
#include "table.h"
#include "field.h"
#include "structure.h"
#include "bufferedrecordset.h"
#include "cell.h"



using namespace std;

namespace tsdb {

/* ====================================================================
 * class Timeseries - Represents a timeseries 
 * ====================================================================
 */

Timeseries::Timeseries() {
	my_split_index_gt = SPLIT_INDEX_GT;
	my_index_step = INDEX_STEP;
}
/** <summary>Timeseries create constructor, with a vector of fields</summary>
 * <remarks><p>Creates a new timeseries with the fields in <c>new_fields</c>. Note that the 
 * <c>new_fields</c> vector should not include a "_TSDB_timestamp" field, as that is prepended 
 * automatically to the start of the vector.</p>
 * <p>When creating the timeseries from a vector of fields, a Structure is generated automatically
 * and is accessable through the getStructure() method. You should use this Structure when working 
 * with records from the Timeseries as it will include the above-mentioned _TSDB_timestamp field.</p>
 * </remarks>
 * <param name="new_loc_id">A HDF5 hid_t (group or file id) where the timeseries will be created</param>
 * <param name="new_name">A name for the timeseries. This name needs to obey HDF5 naming rules.</param>
 * <param name="new_title">The title of the timeseries</param>
 * <param name="new_fields">A vector of fields to include with the timeseries</param>
 */
Timeseries::Timeseries(const hid_t _loc_id, const std::string& _name, const std::string& _title, const std::vector<Field*>& _fields) {
	vector<Field*> fields_with_timestamp;
	my_loc_id = _loc_id;
	my_name = _name;
	my_title = _title;
	my_split_index_gt = SPLIT_INDEX_GT;
	my_index_step = INDEX_STEP;

	/* Does the timeseries exist? */
	if(Timeseries::exists(_loc_id, _name)) {
		throw( TimeseriesException("Timeseries already exists."));
	}

	my_group_id = H5Gcreate1(my_loc_id, my_name.c_str(), 0);

	if(my_group_id < 0) {
		throw( TimeseriesException("Error in H5Gcreate1.") );
	}
	
	// Create the record structure	
	fields_with_timestamp.push_back(new TimestampField("_TSDB_timestamp"));

	for(size_t i=0;i<_fields.size();i++) {
		fields_with_timestamp.push_back(_fields.at(i));
	}
	
	my_structure = boost::make_shared<tsdb::Structure>(fields_with_timestamp, true);

	// Create the data table
	my_data = boost::make_shared<tsdb::Table>(my_group_id, "_TSDB_data", "TSDB: Timeseries Data", my_structure);

	my_buffer_last_ts = LLONG_MIN;


}
/** <summary>Timeseries create constructor, with a pre-defined structure.</summary>
 * <remarks><p>Creates a new timeseries with  structure in <c>new_struct</c>. You must include a 
 * TimestampField called "_TSDB_timestamp" as the first field in your Structure. The constructor will
 * throw an TimeseriesException if this is not the case.</p></remarks>
 * <param name="new_loc_id">A HDF5 hid_t (group or file id) where the timeseries will be created</param>
 * <param name="new_name">A name for the timeseries. This name needs to obey HDF5 naming rules.</param>
 * <param name="new_title">The title of the timeseries</param>
 * <param name="new_struct">A Structure, with a _TSDB_timestamp field as the first field</param>
 */
Timeseries::Timeseries(const hid_t _loc_id, const std::string& _name, const std::string& _title, const boost::shared_ptr<tsdb::Structure>& _structure) {
	my_loc_id = _loc_id;
	my_name = _name;
	my_title = _title;
	my_split_index_gt = SPLIT_INDEX_GT;
	my_index_step = INDEX_STEP;

	/* Does the timeseries exist? */
	if(Timeseries::exists(_loc_id, _name)) {
		throw( TimeseriesException("Timeseries already exists."));
	}
	
	/* Check if the Structure has that_TSDB_timestamp field */
	size_t tsdb_indx_id = _structure->getFieldIndexByName("_TSDB_timestamp");
	if(tsdb_indx_id > 0) {
		throw(TimeseriesException("When creating Timeseries, _TSDB_timestamp is not the first field"));
	}

	if(_structure->getField(0)->getTSDBType() != "Timestamp") {
		throw(TimeseriesException("_TSDB_timestamp field is not a TimestampField."));
	}
	
	// Create the group that holds the time series tables
	my_group_id = H5Gcreate1(my_loc_id, my_name.c_str(), 0);
	if(my_group_id < 0) {
		throw( TimeseriesException("Error in H5Gcreate1.") );
	}
	
	my_structure = _structure;

	// Create the data table
	my_data = boost::make_shared<tsdb::Table>(my_group_id, "_TSDB_data", "TSDB: Timeseries Data", my_structure);

	my_buffer_last_ts = LLONG_MIN;
}

/** <summary>Timeseries open constructor</summary>
 * <remarks><p>Opens an existing timeseries from a hid_t location. This function will throw a 
 * TimeseriesException if the timeseries is not valid.</p></remarks>
 * <param name="grp_loc_id">A HDF5 hid_t (group or file id) where the timeseries exists</param>
 * <param name="grp_name">Name of the timeseries</param>
 */
Timeseries::Timeseries(const hid_t _loc_id, const std::string& _name) {
	hid_t grp_id;

	grp_id = H5Gopen(_loc_id,_name.c_str(),H5P_DEFAULT);

	if(!Table::exists(grp_id,"_TSDB_data")) {
		throw( TimeseriesException("The TSDB data table, '_TSDB_data' does not exist for this timeseries.") );
	}

	my_loc_id = _loc_id;
	my_name = _name;
	my_group_id = grp_id;

	my_data = boost::make_shared<tsdb::Table>(grp_id,"_TSDB_data");
	my_structure = my_data->structure();
	
	my_title = my_data->title();
	my_split_index_gt = SPLIT_INDEX_GT;
	my_index_step = INDEX_STEP;
	
	if(Timeseries::exists(grp_id,"_TSDB_index")) {
		my_index_ts = boost::make_shared<tsdb::Timeseries>(grp_id,"_TSDB_index");
	}

	my_buffer_last_ts = LLONG_MIN;
}

/** <summary>Checks if a Timeseries exists at the specified location</summary>
 * <param name="loc_id">A HDF5 hid_t (group or file id) where the timeseries should be</param>
 * <param name="name">Name of the timeseries</param>
 */
bool Timeseries::exists(hid_t loc_id, std::string name) {
	hid_t grp_id;
	herr_t status;
	/* Error printing off */
	status = H5Eset_auto2(H5E_DEFAULT,NULL,NULL);
	if(status < 0) {
		throw( TimeseriesException("There was a problem redirecting error output."));
	}

	/* Check for containing group */
	grp_id = H5Gopen(loc_id,name.c_str(),H5P_DEFAULT);

	/* Error printing on */
	status = H5Eset_auto2(H5E_DEFAULT,(H5E_auto2_t) H5Eprint, stderr);
	if(status < 0) {
		throw( TimeseriesException("There was a problem redirecting error output."));
	}

	if(grp_id < 0) {
		return false;
	}

	if(!Table::exists(grp_id,"_TSDB_data")) {
		return false;
	} else {
		return true;
	}
}

void Timeseries::setIndexStep(size_t _index_step) {
	my_index_step = _index_step;
}

void Timeseries::setSplitIndexGt(size_t _split_index_gt) {
	my_split_index_gt = _split_index_gt;
}


/** <summary>Appends records at the end of a Timeseries</summary>
 * <remarks><p>When appending records, the records need to come after the last timestamp of the timeseries.
 * If the records are have timestamps prior to the last timestamp, the behaivor of the method
 * is governed by <c>discard_overlap</c>.</p>
 * <p>When <c>discard_overlap</c> is true, records that come prior to the last record (have timestamps strictly less than the
 * last timestamp of the timeseries) are discarded without throwing an exception. In this case, the return value of the 
 * method is the number of records discarded.</p>
 * <p>If <c>discard_overlap</c> is <c>false</c>, then if any timestamps are less than the timestamp of the last
 * record, a TimeseriesExceptions is thrown.</p></remarks>
 * <param name="nrecords">Number of records to be appended</param>
 * <param name="records">Pointer to the records</param>
 * <param name="discard_overlap">If <c>true</c>, appended records that overlap with existing records
 * are discarded.</param>
 */
int Timeseries::appendRecords(size_t nrecords, void* records, bool discard_overlap) {
	
	// No records to append, just return
	if(nrecords == 0) {
		return 0;
	}

	// Cast records as a pointer to char so we can do pointer math.
	char* records_c = (char*) records;
	long long* tsptr = NULL;
	long long prevts = 0;
	long long curts = 0;
	void* last_record = NULL;
	herr_t status;
	size_t record_size = my_structure->getSizeOf();
	size_t i;

	/* Write out some debugging */
	#ifdef DEBUG
		tracer << endl << "appending " << nrecords << " records..." << endl;
		tracer << "start: " << my_structure->structsToString(records,1,",","") << endl;
		tracer << "end: " << my_structure->structsToString(
			my_structure->pointerToMember(records,nrecords-1,0),1,",","") << endl << endl;
	#endif
	if(nrecords > 1) {
		// First determine if the records are sorted
		// NOTE: Assuming the timestamp is offset zero! This speeds up this
		// a lot since there are no methods calls to Structure methods.
	
		tsptr = (timestamp_t*) records_c;
		prevts = *tsptr;

		for(size_t i = 1; i<nrecords; i++) {
			tsptr = (timestamp_t*) (records_c + (record_size*i));
			if(prevts > *tsptr) {
				// Sort the records and break
				tracer << "** records need to be sorted!" << endl;
				qsort(records, nrecords, record_size, tsdb::record_cmp);
				break;
			}
		}
	}

	// Are the records to be added all after the last record in the table?
	tsptr = (timestamp_t*) records_c;
	curts = *tsptr;

	last_record = my_data->getLastRecord();

	if(last_record != NULL) {
		prevts = *((timestamp_t*) last_record);

		if(prevts > curts) {
			// records aren't all after the last timestamp
			if(!discard_overlap) {
				free(last_record);
				
				throw( TimeseriesException("Records are overlapping, and discard_overlap=false.") );
			} else {
			// discard the overlapping records
				for(i=0;i<nrecords;i++) {
					curts = *((timestamp_t*) my_structure->pointerToMember(records,i,0));
					if(curts >= prevts) {
						my_data->appendRecords(nrecords-i,
							my_structure->pointerToMember(records,i,0));
						indexTail();
						free(last_record);
						tracer << "appended " << nrecords-i << " records, but discarded " << i << " records." << endl;
						return i; 
					}
				}

				status = 0;
				free(last_record);
				tracer << "did not append any records, because they all had timestamps < the last timestamp of the series." << endl;
				return i;
			}
		}
	}
	
	my_data->appendRecords(nrecords, records);

	indexTail();

	free(last_record);
	return 0;
}

/** <summary>Destructor</summary>
 * <remarks>Note that Structure linked to this Timeseries is destroyed when this object is 
 * destroyed. This is the case whether it was passed on construction or generated internally.
 * Additionally, the append buffer is flushed to disk.</remarks>
 */
Timeseries::~Timeseries(void) {

}

/** <summary>Returns the Stucture of the Timeseries records</summary> */
boost::shared_ptr<tsdb::Structure> Timeseries::structure(void) {
	return my_structure;
}
/**
 * <summary>
 * Creates and index for the timeseries instance, if the number of rows in the data
 * table is greater than or equal to <c>this->split_index_gt</c>.
 * </summary> <remarks>This method returns
 * <c>FALSE</c> if the index already exists (in which case the function does nothing),
 * and <c>TRUE</c> if the index was created by this function or if the data table is
 * too small for an index.
 * </remarks>
 */
bool Timeseries::createIndexIfNecessary(void) {

	/* Conveience structure for index records */
	#pragma pack(push, index_record, 4)
	typedef struct {
	timestamp_t timestamp;
	hsize_t record_id; } index_record_t;
	#pragma pack(pop, index_record )

	/* If the index already exists, then it is not necessary to create it.
	 * Exit with FALSE. */
	if(my_index_ts.get() != 0) {
		return false;
	}

	/* Is the table big enough for an index */
	hsize_t tbl_nrecords = my_data->size();
	
	/* If no, don't create one yet and return TRUE */
	if(tbl_nrecords <= my_split_index_gt) {
		return true;
	}

	/* An index is necessary, so create it! Start with creating the table. */
	tracer << "Creating a new index for series: " << my_name << ", because it has " << tbl_nrecords << " records." << endl;

	vector<Field*> index_fields;
	vector<size_t> offsets;
	index_fields.push_back(new TimestampField("_TSDB_timestamp"));
	offsets.push_back(offsetof(index_record_t,timestamp));

	index_fields.push_back(new RecordField("record_id"));
	offsets.push_back(offsetof(index_record_t,record_id));

	boost::shared_ptr<tsdb::Structure> indexStructure = 
		boost::make_shared<tsdb::Structure>(index_fields,offsets,sizeof(index_record_t));


	my_index_ts = boost::make_shared<tsdb::Timeseries>(my_group_id,std::string("_TSDB_index"), std::string("TSDB: Index"),indexStructure);
	

	/* Index the already-existing data */
	hsize_t i = 0; // an iterator
	void * tbl_records = NULL;   // pointer to records from the data table
	index_record_t * indx_records = NULL;  // pointer to records from the index table
	timestamp_t *ts1p, *ts2p;    // two timestamp pointers used later on
	//index_record_t indx_record;  // an index record for convience

	/* Start at the index_step records into the data table and move forward,
	 * index_step records at a time. Need to ensure that index points are 
	 * on the FIRST record of a given unique timestamp. This makes searching
	 * using the index much easier in the future. */
	tracer << "   starting at record #" << my_index_step-1 << endl;

	for(i=my_index_step-1;i<tbl_nrecords;i++) {

		/* Check if record is the first of a timestamp (i.e. that the
		 * previous record does not have the same timestamp. */
		my_data->getRecords(i-1,i,&tbl_records); // get this record and previous record
		ts1p = (timestamp_t*) my_structure->pointerToMember(tbl_records,0,0); // pointer to prev ts
		ts2p = (timestamp_t*) my_structure->pointerToMember(tbl_records,1,0); // pointer to this ts

		if(*ts1p != *ts2p) { // are the timestamps different?
			/* record i is a valid index point, so add it */
			indx_records = (index_record_t*) malloc(sizeof(index_record_t));
			indx_records->timestamp = *ts2p;
			indx_records->record_id = i;
			//indx_record.timestamp = *ts2p;
			//indx_record.record_id = i;
			//this->index_ts->getStructure()->setMember(indx_records,0,0,&indx_record.timestamp);
			//this->index_ts->getStructure()->setMember(indx_records,0,1,&indx_record.record_id);
			my_index_ts->appendRecords(1,indx_records, true);
			free(indx_records);
			tracer << "      index point added at record #" << i << endl;

			i = i + my_index_step-1;
			tracer << "       - advance to record #" << i << endl;
			
		}

	}

	// return true since we made an index.
	return true;
}
/** 
 * <summary>
 * Indexes the "tail" of the data table.
 * </summary>
 * <remarks>
 * If records were appended to the table, this function should be run.
 * This function gets the last index point on the table, and scans the rows in the data table after that
 * point, looking for places to insert additional index points.
 * </remarks>
 */

#ifdef _GCC_
/* Conveience structure for index records */
#pragma pack(push, index_record, 4)
typedef struct {
timestamp_t timestamp;
hsize_t record_id; } index_record_t;
#pragma pack(pop, index_record )
#endif
void Timeseries::indexTail(void) {
	
	/* Check if an index is even necessary. Create index returns true is the table is too small
	 * for an index or if there was no index and one was created. If <c>createIndexIfNecessary()</c>
	 * creates an index, then obviously this function does not have to do it. */
	if(createIndexIfNecessary()) {
		return;
	}

	#ifndef _GCC_
	/* Conveience structure for index records */
	#pragma pack(push, index_record, 4)
	typedef struct {
	timestamp_t timestamp;
	hsize_t record_id; } index_record_t;
	#pragma pack(pop, index_record )
	#endif


	tracer << "indexing the tail of series: " << my_name << endl;

	hsize_t blk_start;                     // the start of a block of records from the data table
	vector<index_record_t> indx_records_v; // a vector of index records to add to the index
	index_record_t indx_record;            // an index record to use for convience.
	void * tbl_records = NULL;             // pointer to records from the data table
	timestamp_t prevts = 0;                // two timestamp values to use below
	timestamp_t thists = 0;
	size_t blk_nrecords = 0;               // the number of records the block
	size_t i;							   // a loop counter
	index_record_t * indx_records = NULL;  // pointer to records to add to the index table

	/* Get the last record of the index */
	index_record_t* indx_last_r = (index_record_t*) my_index_ts->dataTable()->getLastRecord();
	//index_record_t indx_last_r;
	//indx_last_r.timestamp = *((timestamp_t*) this->record_struct->pointerToMember(indx_last_r_ptr,0));
	//indx_last_r.record_id = *((hsize_t*) this->record_struct->pointerToMember(indx_last_r_ptr,1));

	/* Start the block to be indexed <c>index_step</c> records ahead of the
	 * last record to be indexed */
	blk_start = indx_last_r->record_id + my_index_step;
	hsize_t tbl_nrecords = my_data->size();
	
	/* Index one block at a time, until the the end of the table is reached */
	while(blk_start<tbl_nrecords) {
		
		// read the first record of the block and the prev record to see
		// if the first record starts a new timestamp.
		my_data->getRecords(blk_start - 1, blk_start, &tbl_records);
		prevts = *((timestamp_t*) my_structure->pointerToMember(tbl_records,0,0));
		thists = *((timestamp_t*) my_structure->pointerToMember(tbl_records,1,0));
		free(tbl_records);
		tracer << "   start processing block at record #" << blk_start << endl;

		// the block starts with a new timestamp?
		if(prevts < thists) {

			// if yes, record this as an index point.
			indx_record.timestamp = thists;
			indx_record.record_id = blk_start;
			indx_records_v.push_back(indx_record);
			tracer << "      index point added at record #" << blk_start << endl;

			// advance the block by one index_step
			blk_start = blk_start + my_index_step;
			tracer << "       - advance to record #" << blk_start << endl;
		
		} else { 
			
			tracer << "      record #" << blk_start << " repeats timestamp. searching for new timstamp" << endl;

			// if no, get up to index_step-1 records from the table. If not index_step-1 avail, get as many as possible
			if(tbl_nrecords - blk_start < ( (hsize_t) my_index_step ) - 1) {
				// there are less then index_step-1 records avail., so get up to the end of the table.
				my_data->getRecords(blk_start, tbl_nrecords-1, &tbl_records);
				blk_nrecords =  ( (size_t) (tbl_nrecords - blk_start) );
			} else {
				// there are at least index_step records left, so get index_step records
				my_data->getRecords(blk_start, blk_start + my_index_step-1 - 1, &tbl_records);
				blk_nrecords = my_index_step-1;
			}
			tracer << "       - the search block has " << blk_nrecords << " records" << endl;

			// scan these records sequentially, finding the first one with a different timestamp.
			i = 0;
			while(prevts == thists && i < blk_nrecords) {
				// increment thists to the timestamp of record i
				thists = *((timestamp_t*) my_structure->pointerToMember(tbl_records,i,0));
				i++;
			}

			// done with those records, so free memory
			free(tbl_records);

			// did the loop exit because of a vaild index point?
			if(prevts < thists) {
				i--; // decrement i so that it refers to the array index number of the record to index

				// record an index point at that record
				indx_record.timestamp = thists;
				indx_record.record_id = blk_start+i;
				indx_records_v.push_back(indx_record);
				tracer << "      index point added at record #" << blk_start+i << endl;
				
				// the next block starts index_step records after that point
				blk_start = indx_record.record_id + my_index_step;
				tracer << "       - advance to record #" << blk_start << endl;
			} else {
				// no vaild index point was found, so move the start of the block up by
				// index_step
				tracer << "   end of block. no valid index points, advancing to record #" << blk_start + my_index_step << endl;
				blk_start = blk_start + my_index_step;
				
			}
		}

	}

	/* Actually add the index points to the index */
	if(indx_records_v.size() > 0) {
		// allocate some memory to store these records
		indx_records = (index_record_t*) malloc(sizeof(index_record_t) * indx_records_v.size());
		// TODO: Can this be replaced with a memcpy, or can we use the internal vector memory directly?
		for(i=0;i<indx_records_v.size();i++) {
			indx_records[i].timestamp = indx_records_v.at(i).timestamp;
			indx_records[i].record_id = indx_records_v.at(i).record_id;
			//this->index_ts->getStructure()->setMember(indx_records,i,0,&indx_record.timestamp);
			//this->index_ts->getStructure()->setMember(indx_records,i,1,&indx_record.record_id);
		}

		// actually add the records
		
		my_index_ts->appendRecords(indx_records_v.size(),indx_records,true);
		
		free(indx_records);
	}

}



/** <summary>Returns the record closest to the given <c>timestamp</c> on the less-than side</summary>
 * <remarks>This function returns the record that is closest to the given <c>timestamp</c> that is
 * equal-to or less-than that <c>timestamp</c>. It is important to note that if there are more
 * than one record with the same matching timestamp, <em>by convention</em> the first (lowest record id) record of this
 * subset is returned. If there are no records that meet the criterion,
 * then the function returns -1 and nothing is written to the <c>record_id</c> memory address.</remarks>
 * <param name="timestamp">Timestamp to search for</param>
 * <param name="record_id">Pointer to a record_id. The resulting record is stored here</param>
 */
herr_t Timeseries::recordId_LE(timestamp_t timestamp, hsize_t* record_id){
	
	
	/* Conveience structure for index records */
	#pragma pack(push, index_record, 4)
	typedef struct {
	timestamp_t timestamp;
	hsize_t record_id; } index_record_t;
	#pragma pack(pop, index_record )

	hsize_t indx_id_LE, indx_id_GE, tbl_first_id, tbl_last_id;
	index_record_t *indx_LE, *indx_GE; // NULL if these don't exist
	void* tbl_records;



	if(my_index_ts.get() != 0 ) {
		if(my_index_ts->recordId_LE(timestamp,&indx_id_LE) == -1) {
			indx_LE = NULL;
			tbl_first_id = 0; // Search from the top of the table then.
		} else {
			indx_LE = (index_record_t*) my_index_ts->getRecordsById(indx_id_LE, indx_id_LE);
			// First of all, if the exact timestamp is in the index, great!
			if(indx_LE->timestamp == timestamp) {
				*record_id = indx_LE->record_id;
				free(indx_LE);
				return 0;
			}
			tbl_first_id = indx_LE->record_id;
		}

		// NOTE: there is no point in checking if the indx_GE is the timestamp we are
		// searching for. It isn't otherwise it would have been returned in indx_LE
		// and we would have returned already.
		if(my_index_ts->recordId_GE(timestamp,&indx_id_GE) == -1) {
			indx_GE = NULL;
			tbl_last_id = my_data->size() -1; // Search to the end then.
		} else {
			indx_GE = (index_record_t*) my_index_ts->getRecordsById(indx_id_GE, indx_id_GE);
			tbl_last_id = indx_GE->record_id;
		}
		free(indx_LE);
		free(indx_GE);	
		
	} else {
		// get the whole table into memory and search it.
		tbl_first_id = 0;
		tbl_last_id = my_data->size() - 1;
	}

	// Now, we know the timestamp we are looking for is likely in the range [tbl_first_id, tbl_last_id]
	// get range of records from table based on those record_ids
	tbl_records = getRecordsById(tbl_first_id,tbl_last_id);

	// Search backward, find the first record that is LE timestamp
	signed long long i;
	timestamp_t thists = 0;
	timestamp_t matchts = 0;
	for(i=tbl_last_id - tbl_first_id;i>=0;i--) {
		thists = *((timestamp_t *) my_structure->pointerToMember(tbl_records,(size_t) i,0));
		if(thists <= timestamp) {
			// Search backwards until we find the first record of a new timestamp.
			matchts = thists;
			for(i=i;i>=0;i--) {
				thists = *((timestamp_t *) my_structure->pointerToMember(tbl_records,(size_t) i,0));
				if(matchts > thists) {
					// found it.
					*record_id = tbl_first_id + i + 1;
					free(tbl_records);
					return 0;
				}
			}
			
			// reached the top of the table, so return record_id = 0
			*record_id = 0;
			free(tbl_records);
			return 0;
		}
	}

	free(tbl_records);

	// OK, didn't find it.
	return -1;
}


/** <summary>Returns the record closest to the given <c>timestamp</c> on the greater-than side</summary>
 * <remarks>This function returns the record that is closest to the given <c>timestamp</c> that is
 * equal-to or greater-than that <c>timestamp</c>. It is important to note that if there are more
 * than on records with the same matching timestamp, <em>by convention</em> the first (lowest record id) record of this
 * subset is returned. If there are no records that meet this criterion,
 * then the function returns -1 and nothing is written to the <c>record_id</c> memory address.</remarks>
 * <param name="timestamp">Timestamp to search for</param>
 * <param name="record_id">Pointer to a record_id. The resulting record is stored here</param>
 */
herr_t Timeseries::recordId_GE(timestamp_t timestamp, hsize_t* record_id){

	/* Conveience structure for index records */
	#pragma pack(push, index_record, 4)
	typedef struct {
	timestamp_t timestamp;
	hsize_t record_id; } index_record_t;
	#pragma pack(pop, index_record )


	hsize_t indx_id_LE, indx_id_GE, tbl_first_id, tbl_last_id;
	index_record_t *indx_LE, *indx_GE; // NULL if these don't exist
	void* tbl_records;

	if(my_index_ts.get() != 0 ) {
		if(my_index_ts->recordId_LE(timestamp,&indx_id_LE) == -1) {
			indx_LE = NULL;
			tbl_first_id = 0; // Search from the top of the table then.
		} else {
			indx_LE = (index_record_t*)my_index_ts->getRecordsById(indx_id_LE, indx_id_LE);
			// First of all, if the exact timestamp is in the index, great!
			if(indx_LE->timestamp == timestamp) {
				*record_id = indx_LE->record_id;
				free(indx_LE);
				return 0;
			}
			tbl_first_id = indx_LE->record_id;
		}

		// NOTE: there is no point in checking if the indx_GE equals the timestamp we are
		// searching for. It isn't otherwise it would have been returned in indx_LE
		// and we would have returned already.
		if(my_index_ts->recordId_GE(timestamp,&indx_id_GE) == -1) {
			indx_GE = NULL;
			tbl_last_id = my_data->size() -1; // Search to the end then.
		} else {
			indx_GE = (index_record_t*) my_index_ts->getRecordsById(indx_id_GE, indx_id_GE);
			tbl_last_id = indx_GE->record_id;
		}
		free(indx_LE);
		free(indx_GE);	
		
	} else {
		// get the whole table into memory and search it.
		tbl_first_id = 0;
		tbl_last_id = my_data->size() - 1;
	}

	// Now, we know the timestamp we are looking for is likely in the range [tbl_first_id, tbl_last_id]
	// get range of records from table based on those record_ids
	tbl_records = this->getRecordsById(tbl_first_id,tbl_last_id);

	// Search forwards, find the first record that is GE timestamp.
	// This record will automatically be the first of that timestamp
	// group.
	size_t i;
	timestamp_t thists = 0;
	timestamp_t matchts = 0;
	for(i=0;i<(tbl_last_id - tbl_first_id)+1;i++) {
		thists = *((timestamp_t*) my_structure->pointerToMember(tbl_records,i,0));
		if(thists >= timestamp) {
			*record_id = tbl_first_id + i;
			free(tbl_records);
			return 0;
		}
			/*
			// Search forwards until we find the first record of a new timestamp.
			matchts = thists;
			for(i=i;i<(tbl_last_id - tbl_first_id)+1;i++) {
				thists = *((timestamp_t*) this->getStructure()->pointerToMember(tbl_records,i,0));
				if(matchts < thists) {
					// found it.
					*record_id = tbl_first_id + i - 1;
					free(tbl_records);
					return 0;
				}
			}
			
		}*/
	}

	free(tbl_records);
	return -1;
}
/** <summary>Returns the record closest to the given <c>timestamp</c> on the greater-than side</summary>
 * <remarks>This function returns the record that is closest to the given <c>timestamp</c> that is
 * equal-to or greater-than that <c>timestamp</c>. It is important to note that if there are more
 * than on records with the same matching timestamp, <em>by convention</em> the first (lowest record id) record of this
 * subset is returned. If there are no records that meet this criterion,
 * then the function returns -1 and nothing is written to the <c>record_id</c> memory address.</remarks>
 * <param name="timestamp">Timestamp to search for</param>
 * <param name="record_id">Pointer to a record_id. The resulting record is stored here</param>
 */
herr_t Timeseries::recordId_GE(boost::posix_time::ptime timestamp, hsize_t *record_id) {
	return this->recordId_GE(tsdb::ptime_to_timestamp(timestamp),record_id);
}

/** <summary>Returns the record closest to the given <c>timestamp</c> on the less-than side</summary>
 * <remarks>This function returns the record that is closest to the given <c>timestamp</c> that is
 * equal-to or less-than that <c>timestamp</c>. It is important to note that if there are more
 * than on records with the same matching timestamp, <em>by convention</em> the first (lowest record id) record of this
 * subset is returned. If there are no records that meet this criterion,
 * then the function returns -1 and nothing is written to the <c>record_id</c> memory address.</remarks>
 * <param name="timestamp">Timestamp to search for</param>
 * <param name="record_id">Pointer to a record_id. The resulting record is stored here</param>
 */
herr_t Timeseries::recordId_LE(boost::posix_time::ptime timestamp, hsize_t *record_id) {
	return this->recordId_LE(tsdb::ptime_to_timestamp(timestamp),record_id);
}

/** <summary>Gets a set of records based on record_ids</summary>
 * <remarks>This function is inclusive of the <c>first</c> and <c>last</c> records.</remarks>
 * <param name="first">First record_id</param>
 * <param name="last">Last record_id</param>
 */
void* Timeseries::getRecordsById(hsize_t first, hsize_t last) {
	void* records;
	my_data->getRecords(first,last,&records);
	return records;
}

/** <summary>Gets a set of records based on timestamps</summary>
 * <remarks>This function is inclusive of the <c>start</c> and <c>end</c> timestamps. This function
 * allocates memory if <c>nrecords</c> is greater than 0, which you must <c>free()</c>.</remarks>
 * <param name="start">Starting timestamp</param>
 * <param name="end">Ending timestamp</param>
 * <param name="nrecords">Pointer to a <c>hsize_t</c> used to save the number of records returned</param>
 * <param name="records">Pointer to a pointer, where the address of the records is saved</param>
 */
void Timeseries::getRecordsByTimestamp(boost::posix_time::ptime start, boost::posix_time::ptime end, hsize_t* nrecords, void** records) {
	tsdb::timestamp_t start_t,end_t;
	start_t = tsdb::ptime_to_timestamp(start);
	end_t = tsdb::ptime_to_timestamp(end);
	this->getRecordsByTimestamp(start_t,end_t,nrecords,records);
}

/** <summary>Gets a set of records based on timestamps</summary>
 * <remarks>This function is inclusive of the <c>start</c> and <c>end</c> timestamps. This function
 * allocates memory if <c>nrecords</c> is greater than 0, which you must <c>free()</c>.</remarks>
 * <param name="start">Starting timestamp</param>
 * <param name="end">Ending timestamp</param>
 * <param name="nrecords">Pointer to a <c>hsize_t</c> used to save the number of records returned</param>
 * <param name="records">Pointer to a pointer, where the address of the records is saved</param>
 */
void Timeseries::getRecordsByTimestamp(tsdb::timestamp_t start, tsdb::timestamp_t end, hsize_t* nrecords, void** records) {
	hsize_t start_id = 0;
	hsize_t end_id = 0;
	herr_t status;


	if(start > end) {
		throw(TimeseriesException("Start timestamp cannot be greater than end timestamp."));
	}

	
	status = this->recordId_GE(start, &start_id);
	if(status == -1) {
		throw(TimeseriesException("The start timestamp is greater then the last record in the timeseries."));
	}

	status = this->recordId_LE(end, &end_id);
	if(status == -1) {
		throw(TimeseriesException("The end timestamp was less than the first record in the timeseries."));
	}

	status = this->recordId_GE(end+1, &end_id);
	if(status == -1) {
		end_id = this->getNRecords() - 1;
	} else {
		end_id = end_id -1;
	}

	if(end_id < start_id) { // no records found
		*nrecords = 0;
		*records = NULL;
		return;
	}

	*nrecords = end_id - start_id + 1;
	*records = this->getRecordsById(start_id,end_id);

}

/** <summary>Gets a RecordSet of records from IDs first to last, inclusive</summary>
 * <param name="first">First record_id</param>
 * <param name="last">Last record_id</param>
 */
tsdb::RecordSet Timeseries::recordSet(hsize_t first, hsize_t last) {
	return my_data->recordSet(first,last);
}

/** <summary>Gets a RecordSet of records from start to end, inclusive</summary>
 * <param name="start">Starting timestamp</param>
 * <param name="end">Ending timestamp</param>
 */
tsdb::RecordSet Timeseries::recordSet(boost::posix_time::ptime start, boost::posix_time::ptime end) {
	return this->recordSet(tsdb::ptime_to_timestamp(start),tsdb::ptime_to_timestamp(end));
}

/** <summary>Gets a RecordSet of records from start to end, inclusive</summary>
 * <param name="start">Starting timestamp</param>
 * <param name="end">Ending timestamp</param>
 */
tsdb::RecordSet Timeseries::recordSet(tsdb::timestamp_t start, tsdb::timestamp_t end) {
	hsize_t start_id = 0;
	hsize_t end_id = 0;
	herr_t status;


	if(start > end) {
		throw(TimeseriesException("Start timestamp cannot be greater than end timestamp."));
	}

	
	status = this->recordId_GE(start, &start_id);
	if(status == -1) {
		throw(TimeseriesException("The start timestamp is greater then the last record in the timeseries."));
	}

	status = this->recordId_LE(end, &end_id);
	if(status == -1) {
		throw(TimeseriesException("The end timestamp was less than the first record in the timeseries."));
	}

	status = this->recordId_GE(end+1, &end_id);
	if(status == -1) {
		end_id = this->getNRecords() - 1;
	} else {
		end_id = end_id -1;
	}

	if(end_id < start_id) { // no records found
		return tsdb::RecordSet(); // TODO: link in the structure. There should be a constructor for RecordSet that
		// makes an empty one that is still linked to the structure.
	}

	return this->recordSet(start_id,end_id);
}


/** <summary>Returns how many records are between timestamps</summary>
 * <remarks>This function is inclusive of the <c>start</c> and <c>end</c> timestamps.</remarks>
 * <param name="start">Starting timestamp</param>
 * <param name="end">Ending timestamp</param>
 */
hsize_t Timeseries::getNRecordsByTimestamp(boost::posix_time::ptime start, boost::posix_time::ptime end) {
	return this->getNRecordsByTimestamp(tsdb::ptime_to_timestamp(start),tsdb::ptime_to_timestamp(end));
}

/** <summary>Returns how many records are between timestamps</summary>
 * <remarks>This function is inclusive of the <c>start</c> and <c>end</c> timestamps.</remarks>
 * <param name="start">Starting timestamp</param>
 * <param name="end">Ending timestamp</param>
 */
hsize_t Timeseries::getNRecordsByTimestamp(tsdb::timestamp_t start, tsdb::timestamp_t end) {
	hsize_t start_id = 0;
	hsize_t end_id = 0;
	herr_t status;


	if(start > end) {
		return 0;
	}

	
	status = this->recordId_GE(start, &start_id);
	if(status == -1) {
		return 0;
	}

	status = this->recordId_LE(end, &end_id);
	if(status == -1) {
		return 0;
	}

	status = this->recordId_GE(end+1, &end_id);
	if(status == -1) {
		end_id = this->getNRecords() - 1;
	} else {
		end_id = end_id -1;
	}

	// This prevents underflows, since start_id and end_id are unsigned.
	if(end_id < start_id) {
		return 0;
	}

	return end_id - start_id + 1;

}

/** <summary>Gets the last record of the timeseries</summary>
 * <remarks>Returns a pointer to the last record of the timeseries. You need to <c>free()</c> this
 * memory.</remarks>
 */
void* Timeseries::getLastRecord() {
	return my_data->getLastRecord();
}

/** <summary>Returns the number of records</summary> */
hsize_t Timeseries::getNRecords() {
	return my_data->size();
}

void Timeseries::appendRecord(tsdb::Record &record) {
		
	tsdb::timestamp_t record_ts  = record[0].toTimestamp();

	if(record_ts >= my_buffer_last_ts) {
		my_data->appendRecord(record);
		if(my_data->appendBufferSize() == 0) {

			my_buffer_last_ts = LLONG_MIN;

			// Buffer must have been flushed, so reindex the tail
			indexTail();
		}
	} else {
		throw std::runtime_error("attempted to append a misordered timestamp");
	}

}

void Timeseries::appendRecordSet(const tsdb::RecordSet &recset, bool discard_overlap) {
	this->appendRecords(recset.size(), recset.memoryBlockPtr().raw(), discard_overlap);
}

const boost::shared_ptr<tsdb::Table>& Timeseries::dataTable() {
	return my_data;
}

/** <summary>Flushes the append buffer to disk</summary> */
void Timeseries::flushAppendBuffer() {
	my_data->flushAppendBuffer();
	my_buffer_last_ts = LLONG_MIN;
	indexTail();
}

/** <summary>Returns a BufferedRecordSet (inclusive)</summary>
 * <param name="first">First record ID to include</param>
 * <param name="last">Last record ID to include</param>
 */
tsdb::BufferedRecordSet Timeseries::bufferedRecordSet(hsize_t first, hsize_t last) {
	return my_data->bufferedRecordSet(first,last);
}

/** <summary>Returns a BufferedRecordSet (inclusive)</summary>
 * <param name="first">First timestamp to include</param>
 * <param name="last">Last timestamp to include</param>
 */
tsdb::BufferedRecordSet Timeseries::bufferedRecordSet(tsdb::timestamp_t start, tsdb::timestamp_t end) {
	hsize_t start_id = 0;
	hsize_t end_id = 0;
	herr_t status;


	if(start > end) {
		return tsdb::BufferedRecordSet();
	}

	
	status = this->recordId_GE(start, &start_id);
	if(status == -1) {
		return tsdb::BufferedRecordSet();
	}

	status = this->recordId_LE(end, &end_id);
	if(status == -1) {
		return tsdb::BufferedRecordSet();
	}

	status = this->recordId_GE(end+1, &end_id);
	if(status == -1) {
		end_id = this->getNRecords() - 1;
	} else {
		end_id = end_id -1;
	}

	// If there are no records in that range, end_id will be before start_id
	if(end_id < start_id) {
		return tsdb::BufferedRecordSet();
	}

	return my_data->bufferedRecordSet(start_id,end_id);
}

/** <summary>Returns a BufferedRecordSet (inclusive)</summary>
 * <param name="first">First timestamp to include</param>
 * <param name="last">Last timestamp to include</param>
 */
tsdb::BufferedRecordSet Timeseries::bufferedRecordSet(boost::posix_time::ptime start, boost::posix_time::ptime end) {
	return this->bufferedRecordSet(tsdb::ptime_to_timestamp(start), tsdb::ptime_to_timestamp(end));
}

/*Timeseries::Timeseries() {
	this->data = NULL;
	this->record_struct = NULL;
	this->group_id = -1;
	this->index_ts = NULL;
	this->name = std::string("");
	this->title = std::string("");
	this->index_step = 0;
	this->split_index_gt = 0;
}

Timeseries::Timeseries(const tsdb::Timeseries &ts) {
	// first, destroy this object
	if(this->data != NULL) {
		delete this->data;
	}

	if(this->record_struct != NULL) {
		delete this->record_struct;
	}

	// Now, copy over the new object
	this->data = ts.data;
	this->record_struct = ts.record_struct;
	this->group_id = ts.group_id;
	this->index_ts = ts.index_ts;
	this->
}*/


/** <summary>Converts a boost::posix_time::ptime to a TSDB timestamp</summary> */
timestamp_t ptime_to_timestamp(boost::posix_time::ptime timestamp) {
	using namespace boost::posix_time;
	using namespace boost::gregorian;
	ptime epoch(date(1970,1,1));
	return ((timestamp - epoch).ticks()) / (time_duration::ticks_per_second()/1000);
}

/** <summary>Compares two records for sorting purposes.</summary>
 * <remarks>See the C <c>qsort()</c> function for details.</remarks>
 */
#ifndef _GCC_
int tsdb::record_cmp(const void *a, const void *b) {
#else
int record_cmp(const void *a, const void *b) {
#endif
	timestamp_t tsA,tsB;
	tsA = *((timestamp_t*) a);
	tsB = *((timestamp_t*) b);
	if(tsA < tsB) { return -1; } else if(tsA == tsB) { return 0; } else { return 1; };
}

} // namespace tsdb
