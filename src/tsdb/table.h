#pragma once

/* STL Classes */
#include <string>
#include <vector>
#include <stdexcept>
#include <cstring>
#include <boost/enable_shared_from_this.hpp>

/* External Libraries */
#include "hdf5.h"

/* TSDB Includes */
#include "field.h"
#include "structure.h"
#include "recordset.h"
#include "tsdb.h"
#include "memoryblockptr.h"

namespace tsdb {
/* Forward declarations */

class BufferedRecordSet;
/* -----------------------------------------------------------------
 * TableException. A runtime exception caused by a Table
 * -----------------------------------------------------------------
 */
class  TableException:
	public std::runtime_error
{
public: 
	TableException(const std::string& what): 
	  std::runtime_error(std::string("TableException: ") + what) {} 
};

/* -----------------------------------------------------------------
 * Table. Represents a HDF5 High Level Table
 * -----------------------------------------------------------------
 */

/** <summary>Represents an HDF5 High Level Table</summary>
 * <remarks>This function wraps a number of HDF5_HL functions in a way that supports 
 * TSDB's dynamic typing system.</remarks>
 */
class  Table
{
public:
	/* Constructors */
	Table(hid_t _loc_id, std::string _name, 
		std::string _title, boost::shared_ptr<tsdb::Structure> _structure);
	Table(hid_t _loc_id, std::string _name);

	/* Methods that get information about the Table */
	std::string title();
	hsize_t size(void);
	boost::shared_ptr<tsdb::Structure> structure(void);

	/* Methods that modify the table's data */
	void appendRecords(size_t nrecords, void* records);
	void appendRecord(tsdb::Record &_record);
	void flushAppendBuffer();
	size_t appendBufferSize();


	/* Methods that retrieve the table's data */
	void getRecords(hsize_t first, hsize_t last, void** records); // TODO: change method name
	tsdb::MemoryBlockPtr recordsAsMemoryBlockPtr(hsize_t first, hsize_t last);
	tsdb::RecordSet recordSet(hsize_t first, hsize_t last);
	tsdb::BufferedRecordSet bufferedRecordSet(hsize_t first, hsize_t last);
	void * getLastRecord(void); // TODO: change method name

	/* Static methods */
	static bool exists(hid_t loc_id, std::string name);
		
	/* Destructor */
	~Table(void);

protected:
	void setHDF5Properties(void);

private:
	/* Properties */
	hid_t my_loc_id;
	std::string my_name;
	std::string my_title;
	boost::shared_ptr<tsdb::Structure> my_structure;
	bool my_is_saved;
	tsdb::MemoryBlock my_append_buffer;
	size_t my_nappendbuf;

};

} // namespace tsdb
