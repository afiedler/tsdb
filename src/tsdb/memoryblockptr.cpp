#include "memoryblockptr.h"

namespace tsdb {
MemoryBlockPtr::MemoryBlockPtr(void)
{
	this->my_offset = 0;
}

MemoryBlockPtr::MemoryBlockPtr(boost::shared_ptr<tsdb::MemoryBlock>& _memory_block, size_t _offset) {
	this->my_memory_block = _memory_block;
	this->my_offset = _offset;
}

MemoryBlockPtr::MemoryBlockPtr(tsdb::MemoryBlockPtr& _memory_block_ptr, size_t _offset) {
	this->my_memory_block = _memory_block_ptr.memoryBlock();
	this->my_offset = _memory_block_ptr.offset() + _offset;
}

char * MemoryBlockPtr::raw() const {
	return this->my_memory_block->raw() + this->my_offset;
}

size_t MemoryBlockPtr::offset() {
	return this->my_offset;
}

const boost::shared_ptr<tsdb::MemoryBlock> MemoryBlockPtr::memoryBlock() {
	return this->my_memory_block;
}
void MemoryBlockPtr::memCpy(void *src, size_t size) {
	memcpy(this->raw(),src,size);
}

size_t MemoryBlockPtr::size() {
	return this->my_memory_block->size() - this->my_offset;
}

MemoryBlockPtr::~MemoryBlockPtr(void)
{
}
}
