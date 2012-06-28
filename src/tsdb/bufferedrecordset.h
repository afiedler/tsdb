#pragma once

#include "tsdb.h"
#include "hdf5.h"
#include "table.h"

namespace tsdb {
class BufferedRecordSet
{
public:
	BufferedRecordSet(void);
	BufferedRecordSet(tsdb::Table* _table, hsize_t _first, hsize_t _last);
	tsdb::Record record(hsize_t i);
	hsize_t firstRecordId();
	hsize_t size();
	void set_my_buffer_direction(bool direction);
	~BufferedRecordSet(void);

private:
	void loadRecords(hsize_t first, size_t nrecords);
	bool my_is_buffer_empty;
	bool my_buffer_direction; //true by default; false indicates reverse buffer
	tsdb::Table* my_table; 
	hsize_t my_first;
	hsize_t my_last;
	hsize_t my_buf_first;
	size_t my_nbufrecords;
	tsdb::MemoryBlockPtr my_buffer_ptr;
	size_t my_record_size;
	size_t my_BUFFER_SIZE;
};
}
