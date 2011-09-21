#pragma once
#include "structure.h"
#include "tsdb.h"
#include "hdf5.h"
#include "record.h"
#include "memoryblockptr.h"

namespace tsdb {
class RecordSet
{
public:
	RecordSet(tsdb::MemoryBlockPtr& _record_block, size_t _nrecords, boost::shared_ptr<tsdb::Structure>& _structure);
	RecordSet(size_t _nrecords, boost::shared_ptr<tsdb::Structure>& _structure);
	RecordSet();
	const tsdb::MemoryBlockPtr& memoryBlockPtr(void) const;
	tsdb::Record operator[](size_t i);
	size_t size(void) const;
	~RecordSet(void);
protected:
	size_t my_nrecords;
	tsdb::MemoryBlockPtr my_memory_block_ptr;
	boost::shared_ptr<tsdb::Structure> my_structure;
};
}
