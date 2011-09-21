#pragma once

#include <boost/shared_array.hpp>


namespace tsdb {

class MemoryBlock
{
public:
	MemoryBlock(void);
	MemoryBlock(size_t _size);
	~MemoryBlock(void);
	size_t size();
	boost::shared_array<char> sharedArray();
	char* raw();
	bool isAllocated() const;
private:
	boost::shared_array<char> my_memory;
	size_t my_size;
};

}
