#include "recordset.h"
#include "record.h"
#include "structure.h"
#include "memoryblock.h"

#include <boost/make_shared.hpp>

namespace tsdb {
RecordSet::RecordSet(tsdb::MemoryBlockPtr& _memory_block_ptr, size_t _nrecords, boost::shared_ptr<tsdb::Structure>& _structure) {
	this->my_memory_block_ptr = _memory_block_ptr;
	this->my_structure = _structure;
	this->my_nrecords = _nrecords;
}

RecordSet::RecordSet(size_t _nrecords, boost::shared_ptr<tsdb::Structure>& _structure) {
	this->my_nrecords = _nrecords;
	this->my_structure = _structure;

	// Allocate a MemoryBlock to use, and save a MemoryBlockPtr to it
	boost::shared_ptr<MemoryBlock> memblk =
		boost::make_shared<tsdb::MemoryBlock>(this->my_structure->getSizeOf() * this->my_nrecords);
	this->my_memory_block_ptr = tsdb::MemoryBlockPtr(memblk,0);
}


tsdb::Record RecordSet::operator[](size_t i) {
	tsdb::MemoryBlockPtr recordptr = tsdb::MemoryBlockPtr(
		this->my_memory_block_ptr,
		this->my_structure->getSizeOf() * i);
	return tsdb::Record(recordptr, this->my_structure);
}


/** 
 */
RecordSet::~RecordSet(void) {

}

RecordSet::RecordSet() {
	this->my_nrecords=0;
}

size_t RecordSet::size() const {
	return this->my_nrecords;
}

const tsdb::MemoryBlockPtr& RecordSet::memoryBlockPtr(void) const {
	return this->my_memory_block_ptr;
}
} // namespace tsdb