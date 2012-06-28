#pragma once
#include "tsdb.h"
#include "structure.h"
#include "memoryblockptr.h"

#include <boost/shared_ptr.hpp>

#include <string>
#include <stdexcept>

namespace tsdb {

/* Forward declaration of Cell */
class Cell;

class record_structure_mismatch : public std::runtime_error {
public:
	record_structure_mismatch(const std::string& ehat_arg) : std::runtime_error(ehat_arg) {}
};

/** <summary>Represents a record, either as part of a RecordSet or a "free" record</summary>
 * <remarks>There are two types of Records: (1) Records that are linked to a RecordSet (called "linked"
 * records), and (2) Records that are unlinked (called "free"). If a record is linked to a RecordSet, the
 * RecordSet manages memory, in the interest of efficieny. Record objects just point into the block of memory
 * allocated by the RecordSet, and do no memory management on their own. The second type of Records are
 * are given a pointer to their own block of memory on construction. They manage this block of memory 
 * throughout their lifecycle.</remarks>
 */
class Record
{
public:
	Record(void);
	Record(tsdb::MemoryBlockPtr& _memory_block_ptr, boost::shared_ptr<tsdb::Structure> _structure);
	Record(const boost::shared_ptr<tsdb::Structure>& _structure);
	tsdb::Cell operator[](size_t i); 
	void copyValues(const tsdb::Record& lhs);
	const boost::shared_ptr<tsdb::Structure> structure() const;
	const tsdb::MemoryBlockPtr& memoryBlockPtr() const;
	~Record(void);
private:
	boost::shared_ptr<tsdb::Structure> my_structure;
	tsdb::MemoryBlockPtr my_memory_block_ptr;
};
}
