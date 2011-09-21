#include "memoryblock.h"

namespace tsdb {
MemoryBlock::MemoryBlock(void)
{
	my_size = 0;
}

MemoryBlock::MemoryBlock(size_t _size) {
	this->my_memory = boost::shared_array<char>(new char[_size]);
	this->my_size = _size;
}

size_t MemoryBlock::size() {
	return this->my_size;
}

boost::shared_array<char> MemoryBlock::sharedArray() {
	return this->my_memory;
}


char* MemoryBlock::raw() {
	return this->my_memory.get();
}

bool MemoryBlock::isAllocated() const {
	if(my_memory.get() == 0) {
		return false;
	} else {
		return true;
	}
}

MemoryBlock::~MemoryBlock(void)
{
}
}
