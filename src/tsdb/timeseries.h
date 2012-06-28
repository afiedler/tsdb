#pragma once

/* STL Classes */
#include <string>
#include <vector>
#include <stdexcept>

/* External Libraries */
#include "hdf5.h"
#include "hdf5_hl.h"
#include "boost/date_time/posix_time/posix_time.hpp"

/* TSDB Includes */
#include "tsdb.h"
#include "table.h"
#include "structure.h"
#include "recordset.h"




namespace tsdb {

/* -----------------------------------------------------------------
 * Typedefs/Forward Declarations. 
 * -----------------------------------------------------------------
 */
class BufferedRecordSet;

/* -----------------------------------------------------------------
 * TimeseriesException. For runtime errors thrown by the Timeseries 
 * class.
 * -----------------------------------------------------------------
 */
class  TimeseriesException: 
	public std::runtime_error 
{ 
public:

	TimeseriesException(const std::string& what):
	  std::runtime_error(std::string("TimeseriesException: ") + what) {} 

};

/* -----------------------------------------------------------------
 * Timeseries. Represents a timeseries object in the database
 * -----------------------------------------------------------------
 */

/** <summary>Represents a Table ordered ascending by timestamp</summary>
 * <remarks><p>The Timeseries object is a Table with records in ascending order by a timestamp.
 * By convention, the timestamp must be the first field in the record, and is called "_TSDB_timestamp".
 * All operations on the Timeseries must leave the table sorted, so that each subsequent timestamp
 * is greater-than or equal-to the previous one.</p>
 * <p>Repeated timestamps are allowed, but the database does not preserve the order of repeated 
 * timestamps. When querying the database using the record_LE() and record_GE() methods, by convention,
 * the <em>first</em> record id of a block of records with the same timestamp is returned.</p>
 * <p>The Timeseries object supports sparse indexes. Not every record is included in the index, which
 * saves space and takes advantage of the fact that the records are already sorted. An index is created 
 * when the number of records in the Timeseries surpasses SPLIT_INDEX_GT.</p>
 * <p>The Timeseries is saved in HDF5 as a Group with a Table nested inside. The Group has the the 
 * timeseries name, while the Table is called "_TSDB_data" and actually stores the records. When an index
 * is created, another timeseries is created in the original Group. This timeseries is called "_TSDB_index"
 * and is a full-fledged Timeseries object itself. After the index is created, operations on the 
 * base timeseries will use the index to speed up locating timestamps. Because the index is a Timeseries
 * itself, after it gets too large, it will spawn a secondary index. Queries on the original base Timeseries 
 * will recursively descend the index tree.</p></remarks>
 */
class  Timeseries 
{
public:
	/* Constructors */
	Timeseries(const hid_t _loc_id, const std::string& _name, const std::string& _title, 
		const std::vector<Field*>& _fields);
	Timeseries(const hid_t _loc_id, const std::string& _name, const std::string& _title,
		const boost::shared_ptr<tsdb::Structure>& _structure);
	Timeseries(const hid_t _loc_id, const std::string& _name);
	Timeseries();

	/* Methods to write data to the Timeseries */
	int appendRecords(size_t nrecords, void* records, bool discard_overlap);
	void appendRecordSet(const tsdb::RecordSet &recset, bool discard_overlap);
	void appendRecord(tsdb::Record &record);
	void flushAppendBuffer();

	/* Methods to lookup records in the Timeseries */
	herr_t recordId_LE(timestamp_t timestamp, hsize_t* record_id);
	herr_t recordId_GE(timestamp_t timestamp, hsize_t* record_id);
	herr_t recordId_LE(boost::posix_time::ptime timestamp, hsize_t* record_id);
	herr_t recordId_GE(boost::posix_time::ptime timestamp, hsize_t* record_id);
	void* getLastRecord(void);
	void* getRecordsById(hsize_t first, hsize_t last);
	tsdb::RecordSet recordSet(hsize_t first, hsize_t last);
	tsdb::RecordSet recordSet(tsdb::timestamp_t start, tsdb::timestamp_t end);
	tsdb::RecordSet recordSet(boost::posix_time::ptime start, boost::posix_time::ptime end);
	void getRecordsByTimestamp(boost::posix_time::ptime start, boost::posix_time::ptime end,  hsize_t* nrecords, void** records);
	void getRecordsByTimestamp(tsdb::timestamp_t start, tsdb::timestamp_t end,  hsize_t* nrecords, void** records);
	tsdb::BufferedRecordSet bufferedRecordSet(hsize_t first, hsize_t last);
	tsdb::BufferedRecordSet bufferedRecordSet(tsdb::timestamp_t start, tsdb::timestamp_t end);
	tsdb::BufferedRecordSet bufferedRecordSet(boost::posix_time::ptime start, boost::posix_time::ptime end);
	
	/* Methods to get information about the Timeseries */
	hsize_t getNRecords(void);
	boost::shared_ptr<tsdb::Structure> structure(void);
	hsize_t getNRecordsByTimestamp(boost::posix_time::ptime start, boost::posix_time::ptime end);
	hsize_t getNRecordsByTimestamp(tsdb::timestamp_t start, tsdb::timestamp_t end);
	const boost::shared_ptr<tsdb::Table>& dataTable();

	/* Methods to change the behaivor of the Timeseries */
	void setIndexStep(size_t _index_step);
	void setSplitIndexGt(size_t _split_index_gt);
	/* Static Methods */
	static bool exists(hid_t loc_id, std::string name);

	/* Destructor */
	~Timeseries(void);


private:
	
	/* Private methods to deal with the Timeseries' index */
	bool createIndexIfNecessary(void);
	void indexTail(void);

	/* Properties */
	boost::shared_ptr<tsdb::Table> my_data;
	boost::shared_ptr<tsdb::Structure> my_structure;
	hid_t my_loc_id;
	hid_t my_group_id;
	std::string my_name;
	std::string my_title;
	size_t my_split_index_gt;	// Create an index when more than this nrecords 
	size_t my_index_step;		// Add index points at approx. this step size
	boost::shared_ptr<tsdb::Timeseries> my_index_ts;
	tsdb::timestamp_t my_buffer_last_ts;

};

/* Independent functions relating to timeseries */
int record_cmp(const void * a, const void * b);
timestamp_t ptime_to_timestamp(boost::posix_time::ptime timestamp);

} // namespace tsdb

