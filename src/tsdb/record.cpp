#include "record.h"
#include "structure.h"
#include "cell.h"

#include <boost/make_shared.hpp>

namespace tsdb {

/** <summary>Creates a new Record, defaults to unmanaged memory (linked records)</summary>
 * <remarks>This function creates a new Record object that points to memory 
 * that's part of a larger memory block. The Record will not manage this memory: when
 * the Record is deleted, the memory will not be freed.</remarks>
 * <param name="new_record">Pointer to the memory block used by this Record</param>
 * <param name="new_struct">Pointer to the Structure of the Record</param>
 */
Record::Record(tsdb::MemoryBlockPtr& _memory_block_ptr, boost::shared_ptr<tsdb::Structure> _structure)
{
	my_memory_block_ptr = _memory_block_ptr;
	my_structure = _structure;
}

Record::Record(const boost::shared_ptr<tsdb::Structure>& _structure) {
	my_structure = _structure;
	boost::shared_ptr<tsdb::MemoryBlock> memblk = boost::make_shared<tsdb::MemoryBlock>(
		my_structure->getSizeOf());
	my_memory_block_ptr = tsdb::MemoryBlockPtr(memblk,0);
}

/** <summary>Returns a cell from field index i</summary>
 * <param name="i">Field to return Cell from (counts from 0)</param>
 */
tsdb::Cell Record::operator[](size_t i) {
	tsdb::MemoryBlockPtr cellmemblkptr = tsdb::MemoryBlockPtr(
		my_memory_block_ptr,
		my_structure->getOffsetOfField(i));
	return tsdb::Cell(cellmemblkptr,my_structure->getField(i)->getFieldType(),my_structure->getField(i)->getSizeOf());
}

/** <summary>Returns a pointer to the Record's Structure</summary>
 */
const boost::shared_ptr<tsdb::Structure> Record::structure() const {
	return this->my_structure;
}

/** <summary>Returns a pointer to the underlying block of memory used by the
 * Record.</summary>
 */
const tsdb::MemoryBlockPtr& Record::memoryBlockPtr() const {
	return this->my_memory_block_ptr;
}

Record::~Record(void) {
}

/** <summary>Creates an empty Record</summary>
 */
Record::Record(void) {

}

void Record::copyValues(const tsdb::Record &lhs) {
	if(my_structure.get() != lhs.structure().get()) {
		throw record_structure_mismatch("unable to copy values because record structures don't match");
	}

	memcpy(my_memory_block_ptr.raw(),lhs.memoryBlockPtr().raw(),my_structure->getSizeOf());
}


/** <summary>Copy constructor<summary>
 * <remarks>How a record is copied depends on if its a "free" record or a "linked" record.
 * If its free, then the underlying memory is duplicated and the new record will be completely
 * independent. Otherwise, if the record is linked, the new record will point to the same
 * memory</remarks>
 
Record::Record(const tsdb::Record &r) {
	if(r._mem_belongs) {
		// allocate some new memory for the copy
		this->record = malloc(r.structure()->getSizeOf());
		if(this->record == NULL) {
			throw std::runtime_error("out of memory");
		}
		memcpy(this->record,r.record,r.structure()->getSizeOf());
		this->_mem_belongs = true;
	} else {
		// just point to the same memory
		this->record = r.record;
		this->_mem_belongs = false;
	}

	this->record_struct = r.structure();
}*/

/** <summary>Record assignment</summary>
 * <remarks>Assigns a Record to replace this one. Note, if the assigned record is a linked
 * record, both records will then be linked to the same memory block.</remarks>
 *
tsdb::Record& Record::operator =(const tsdb::Record &r) {
	// replaces this with the other object
	if(this->_mem_belongs) {
		free(this->record);
		this->record = NULL;
	}
	if(r._mem_belongs) {
		// allocate some new memory for the copy
		this->record = malloc(r.structure()->getSizeOf());
		if(this->record == NULL) {
			throw std::runtime_error("out of memory");
		}
		memcpy(this->record,r.record,r.structure()->getSizeOf());
		this->_mem_belongs = true;
	} else {
		// just point to the same memory
		this->record = r.record;
		this->_mem_belongs = false;
	}

	this->record_struct = r.structure();
	return *this;
}*/

}
