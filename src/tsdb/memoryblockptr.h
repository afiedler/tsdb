#pragma once


#include <boost/shared_ptr.hpp>
#include "memoryblock.h"

#ifdef _GCC_
#include <cstring>
#endif

namespace tsdb {
class MemoryBlockPtr
{
public:
	MemoryBlockPtr(void);
	MemoryBlockPtr(boost::shared_ptr<tsdb::MemoryBlock>& _memory_block, size_t _offset);
	MemoryBlockPtr(tsdb::MemoryBlockPtr& _memory_block_ptr, size_t _offset);
	~MemoryBlockPtr(void);
	char* raw() const;
	size_t offset();
	const boost::shared_ptr<tsdb::MemoryBlock> memoryBlock();
	size_t size();
	void memCpy(void* src, size_t size);
private:
	boost::shared_ptr<tsdb::MemoryBlock> my_memory_block;
	size_t my_offset;
};
}
