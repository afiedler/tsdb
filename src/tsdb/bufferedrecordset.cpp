#include "bufferedrecordset.h"
#include <boost/make_shared.hpp>

namespace tsdb {

/** <summary>Create a new BufferedRecordSet linking to a set fo rows on a Table</summary>
 * <param name="table">Pointer to a Table to link to</param>
 * <param name="first">First record ID from table</param>
 * <param name="last">Last record ID from table</param>
 */
BufferedRecordSet::BufferedRecordSet(tsdb::Table* _table, hsize_t _first, hsize_t _last)
{
	this->my_first = _first;
	this->my_last = _last;
	this->my_table = _table;

	// Initialize primative variables
	this->my_buf_first = 0;
	this->my_nbufrecords = 0;
	this->my_record_size = my_table->structure()->getSizeOf();
	this->my_BUFFER_SIZE = 65000;
	this->my_is_buffer_empty = false;
	this->my_buffer_direction = true;
}

BufferedRecordSet::BufferedRecordSet(void) {
	// Initialize primative variables
	this->my_buf_first = 0;
	this->my_nbufrecords = 0;
	this->my_record_size = 0;
	this->my_BUFFER_SIZE = 65000;
	this->my_is_buffer_empty = true;
	this->my_buffer_direction = true;
}

hsize_t BufferedRecordSet::firstRecordId() {
	return this->my_first;
}

//false indicates reverse buffer
void BufferedRecordSet::set_my_buffer_direction(bool direction)
{
	this->my_buffer_direction = direction;
}


/** <summary>Returns a Record at index i</summary>
 * <remarks>Returns the i-th Record. The index starts from 0, and refers to the index in the
 * BufferedRecordSet (not the Record's index in the Table).</remarks>
 * <param name="i">Record index</param>
 */
tsdb::Record BufferedRecordSet::record(hsize_t i) {	
	if(this->my_is_buffer_empty) {
		throw std::runtime_error("empty BufferedRecordSet");
	}

	/* is the index valid? */
	if(i <= (this->my_last - this->my_first)) {
		/* is it in the buffer? */
		if(this->my_buffer_ptr.memoryBlock() != 0 &&
			i >= this->my_buf_first &&
			i <= (this->my_buf_first + this->my_nbufrecords-1)) {
		
			// do nothing

		} else {

			// get a new buffer
			this->loadRecords(i,this->my_BUFFER_SIZE);

		}

		// allocate some new memory, copy the record over, and wrap it in a Record object
		boost::shared_ptr<tsdb::MemoryBlock> recmemblk = 
			boost::make_shared<tsdb::MemoryBlock>(my_record_size);
		tsdb::MemoryBlockPtr recblkptr = tsdb::MemoryBlockPtr(recmemblk,0);
		recblkptr.memCpy(this->my_buffer_ptr.raw() + (i-this->my_buf_first)*this->my_record_size,
			this->my_record_size);
		return tsdb::Record(recblkptr,boost::shared_ptr<tsdb::Structure>(my_table->structure()));

	} else {
		throw std::runtime_error("index out of bounds");
	}
}

void BufferedRecordSet::loadRecords(hsize_t first, size_t nrecords) {
	//loading the records depends on the buffer direction
	if (this->my_buffer_direction)
	{
		// here, we assume that first starts the buffer set within the record
		// nrecords is trimmed if it would put it outside the end of the BufferedRecordSet
		if(this->my_first+first+nrecords-1 > this->my_last) {
			nrecords = (size_t) (this->my_last - (this->my_first+first) + 1);
		}

		this->my_buffer_ptr = my_table->recordsAsMemoryBlockPtr(
			this->my_first+first,this->my_first+first+nrecords-1);


		this->my_buf_first = first;
		this->my_nbufrecords = nrecords;
	}
	else
	{
		// here, we assume that first starts the buffer set within the record
		// nrecords is trimmed if it would put it outside the beginning of the BufferedRecordSet
		if(first < (nrecords-1)) 
			nrecords = (size_t) (first+1);

		this->my_buffer_ptr = my_table->recordsAsMemoryBlockPtr(
			this->my_first+first-(nrecords-1),this->my_first+first);

		this->my_buf_first = first-(nrecords-1);
		this->my_nbufrecords = nrecords;
	}
}

BufferedRecordSet::~BufferedRecordSet(void)
{

}

/** <summary>Returns the number of records in the BufferedRecordSet</summary>
 */
hsize_t BufferedRecordSet::size() {
	if(my_is_buffer_empty) {
		return 0;
	}

	return my_last - my_first + 1;
}
}// namespace tsdb
