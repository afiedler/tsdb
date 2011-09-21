#pragma once

#include "tsdb.h"
#include "field.h"
#include "record.h"
#include <string>
#include <ostream>

namespace tsdb {

class type_conversion_error : public std::runtime_error {
public:
	type_conversion_error(const std::string& ehat_arg) : std::runtime_error(ehat_arg) {}
};

class Cell
{
	
	friend std::ostream& operator<<(std::ostream &stream, const tsdb::Cell& ob);

public:
	Cell(tsdb::MemoryBlockPtr& _memory_block_ptr, tsdb::Field::FieldType _field_type);
	Cell(tsdb::MemoryBlockPtr& _memory_block_ptr, tsdb::Field::FieldType _field_type, size_t _size);
	Cell(tsdb::Field::FieldType _field_type);
	Cell();
	
	/* Assignment operators */
	tsdb::Cell& operator=(const std::string &rhs);
	tsdb::Cell& operator=(const tsdb::ieee64_t &rhs);
	tsdb::Cell& operator=(const tsdb::int32_t &rhs);
	tsdb::Cell& operator=(const tsdb::int8_t &rhs);
	tsdb::Cell& operator=(const tsdb::char_t &rhs);
	tsdb::Cell& operator=(const tsdb::uint64_t &rhs);
	tsdb::Cell& operator=(const tsdb::int64_t &rhs);
	
	/* Conversions */
	tsdb::ieee64_t toDouble() const;
	std::string toString() const ;
	tsdb::int32_t toInt32() const;
	tsdb::date_t toDate() const;
	tsdb::char_t toChar() const;
	tsdb::int8_t toInt8() const;
	tsdb::timestamp_t toTimestamp() const;
	tsdb::record_t toRecordId() const;

	/* Getters */
	tsdb::Field::FieldType fieldType() const;
	

private:
	tsdb::MemoryBlockPtr my_memory_block_ptr;
	tsdb::Field::FieldType my_field_type;
	size_t my_size;

};



} // namespace tsdb

