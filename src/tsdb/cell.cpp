#include "cell.h"
#include "record.h"
#include <string>
#include <sstream>
#include <ostream>

#include "boost/date_time/posix_time/posix_time.hpp"
#include <boost/make_shared.hpp>
#include "memoryblock.h"
#include "memoryblockptr.h"

namespace tsdb {


Cell::Cell(tsdb::MemoryBlockPtr& _memory_block_ptr, tsdb::Field::FieldType _field_type) {
	this->my_memory_block_ptr = _memory_block_ptr;
	this->my_field_type = _field_type;
	this->my_size = 0;
}

Cell::Cell(tsdb::MemoryBlockPtr& _memory_block_ptr, tsdb::Field::FieldType _field_type, size_t _size) {
	this->my_memory_block_ptr = _memory_block_ptr;
	this->my_field_type = _field_type;
	this->my_size = _size;
}

Cell::Cell(tsdb::Field::FieldType _field_type) {
	this->my_field_type = _field_type;

	/* Allocate some memory */
	boost::shared_ptr<tsdb::MemoryBlock> memblk;
	switch(this->my_field_type) {
		case tsdb::Field::CHAR:
			memblk = boost::make_shared<tsdb::MemoryBlock>(sizeof(tsdb::char_t));
			this->my_memory_block_ptr = tsdb::MemoryBlockPtr(memblk,0);
			break;
		case tsdb::Field::DATE:
			memblk = boost::make_shared<tsdb::MemoryBlock>(sizeof(tsdb::date_t));
			this->my_memory_block_ptr = tsdb::MemoryBlockPtr(memblk,0);
			break;
		case tsdb::Field::DOUBLE:
			memblk = boost::make_shared<tsdb::MemoryBlock>(sizeof(tsdb::ieee64_t));
			this->my_memory_block_ptr = tsdb::MemoryBlockPtr(memblk,0);
			break;
		case tsdb::Field::INT32:
			memblk = boost::make_shared<tsdb::MemoryBlock>(sizeof(tsdb::int32_t));
			this->my_memory_block_ptr = tsdb::MemoryBlockPtr(memblk,0);
			break;
		case tsdb::Field::INT8:
			memblk = boost::make_shared<tsdb::MemoryBlock>(sizeof(tsdb::int8_t));
			this->my_memory_block_ptr = tsdb::MemoryBlockPtr(memblk,0);
			break;
		case tsdb::Field::RECORD:
			memblk = boost::make_shared<tsdb::MemoryBlock>(sizeof(tsdb::record_t));
			this->my_memory_block_ptr = tsdb::MemoryBlockPtr(memblk,0);
			break;
		case tsdb::Field::TIMESTAMP:
			memblk = boost::make_shared<tsdb::MemoryBlock>(sizeof(tsdb::timestamp_t));
			this->my_memory_block_ptr = tsdb::MemoryBlockPtr(memblk,0);
			break;
		default:
			this->my_field_type = tsdb::Field::UNDEFINED;
			break;
	}
}


Cell::Cell() {
}



/** <summary>Returns the value of a Cell as a string</summary>
 * <remarks><p>All FieldTypes can be converted to a string.
 * For date and timestamp fields, the string will be an ISO formatted string.
 * For chars, the string will be the 8-bit character.</p><p>For string fields, 
 * extracting the string from a string field requires that the cell know the
 * size of the string. This should be passed to the cell on construction by using
 * the Cell(tsdb::MemoryBlockPtr& _memory_block_ptr, tsdb::Field::FieldType _field_type, 
 * size_t _size) constructor (this is the case when you create a cell from a record).
 * If you create the cell manually and do not call this constructor, you will get a
 * null string.</p></remarks>
 */
std::string Cell::toString() const {
	using namespace boost::gregorian;
	using namespace boost::posix_time;

	std::stringstream ss;
	boost::gregorian::date date_epoch;
	boost::posix_time::time_facet * ptimeFormat;
	ptime thistime;
	ptime epoch;
	std::stringstream ss1;

	switch(this->my_field_type) {
		case tsdb::Field::CHAR:
			tsdb::char_t tmp_char;
			tmp_char = *((tsdb::char_t*) this->my_memory_block_ptr.raw());
			ss << tmp_char;
			break;
		case tsdb::Field::DATE:
			tsdb::date_t tmp_date;
			tmp_date = *((tsdb::date_t*) this->my_memory_block_ptr.raw());
			date_epoch = boost::gregorian::date(1970,1,1);
			ss << to_iso_extended_string(date_epoch + boost::gregorian::date_duration(tmp_date));
			break;
		case tsdb::Field::DOUBLE:
			tsdb::ieee64_t tmp_double;
			tmp_double = *((tsdb::ieee64_t*) this->my_memory_block_ptr.raw());
			ss << tmp_double;
			break;
		case tsdb::Field::INT32:
			tsdb::int32_t tmp_int32;
			tmp_int32 = *((tsdb::int32_t*) this->my_memory_block_ptr.raw());
			ss << tmp_int32;
			break;
		case tsdb::Field::INT8:
			int tmp_int;
			tmp_int = (int) (*((tsdb::int8_t*) this->my_memory_block_ptr.raw()));
			ss << tmp_int;
			break;
		case tsdb::Field::RECORD:
			tsdb::record_t tmp_recordid;
			tmp_recordid = *((tsdb::record_t*) this->my_memory_block_ptr.raw());
			ss << "0x" << std::hex << tmp_recordid;
			break;
		case tsdb::Field::TIMESTAMP:
			timestamp_t tmp_timestamp;
			tmp_timestamp = *((tsdb::timestamp_t*) this->my_memory_block_ptr.raw());

			epoch = ptime(date(1970,1,1));
			/* NOTE: This will overflow at some point, likely year 70021, at which point I
			 * won't worry about it! */
			thistime = epoch + seconds((long)(tmp_timestamp / 1000)) + milliseconds((long)(tmp_timestamp % 1000));
			//using a time_facet for formatting, for standardized timestamp strings
			ptimeFormat = new ::boost::posix_time::time_facet;
			ss1.imbue(std::locale(std::locale::classic(),ptimeFormat));
			ptimeFormat->format("%Y-%m-%dT%H:%M:%S.%f");		
			ss1 << thistime;
			//truncating to millisecond precision
			ss << ss1.str().substr(0,23);
			break;
		case tsdb::Field::STRING:
			char* tmpstr;
			tmpstr = new char[my_size+1];
			memset(tmpstr,0,my_size+1);
			memcpy(tmpstr,this->my_memory_block_ptr.raw(),my_size);
			ss << tmpstr;
			delete[] tmpstr;
			break;
		default:
			ss << "Undef";
			break;
	}

	return ss.str();
}

/** <summary>Returns the value of a Cell as a double</summary>
 * <remarks>FieldTypes of DOUBLE, INT32, INT8, TIMESTAMP, and DATE can be converted to doubles.</remarks>
 */
tsdb::ieee64_t Cell::toDouble() const {
	switch(this->my_field_type) {
		case tsdb::Field::DOUBLE:
			return *((tsdb::ieee64_t*) this->my_memory_block_ptr.raw());
			break;
		case tsdb::Field::INT32:
			return (tsdb::ieee64_t) *((tsdb::int32_t*) this->my_memory_block_ptr.raw());
			break;
		case tsdb::Field::INT8:
			return (tsdb::ieee64_t) *((tsdb::int8_t*) this->my_memory_block_ptr.raw());
			break;
		case tsdb::Field::TIMESTAMP:
			return (tsdb::ieee64_t) *((tsdb::timestamp_t*) this->my_memory_block_ptr.raw());
			break;
		case tsdb::Field::DATE:
			return (tsdb::ieee64_t) *((tsdb::date_t*) this->my_memory_block_ptr.raw());
			break;
		default:
			throw tsdb::type_conversion_error("cannot convert type to double");
			break;
	}
}

/** <summary>Returns the value of a Cell as a 32-bit integer (tsdb::int32_t)</summary>
 * <remarks>FieldTypes of INT32, INT8 and DATE can be converted to int32_t.
 * Others throw type_conversion_error.</remarks>
 */
tsdb::int32_t Cell::toInt32() const {
	switch(this->my_field_type) {
		case tsdb::Field::INT32:
			return *((tsdb::int32_t*) this->my_memory_block_ptr.raw());
			break;
		case tsdb::Field::INT8:
			return (tsdb::int32_t) *((tsdb::int8_t*) this->my_memory_block_ptr.raw());
			break;
		case tsdb::Field::DATE:
			return (tsdb::int32_t) *((tsdb::date_t*) this->my_memory_block_ptr.raw());
			break;
		default:
			throw tsdb::type_conversion_error("cannot convert type to int32");
			break;
	}
}

/** <summary>Returns the value of a Cell as a 8-bit integer (tsdb::int8_t)</summary>
 * <remarks>FieldTypes of INT8 only can be converted to int8_t. Others throw type_conversion_error.</remarks>
 */
tsdb::int8_t Cell::toInt8() const {
	switch(this->my_field_type) {
		case tsdb::Field::INT8:
			return *((tsdb::int8_t*) this->my_memory_block_ptr.raw());
			break;
		default:
			throw tsdb::type_conversion_error("cannot convert type to int8");
			break;
	}
}

/** <summary>Returns the value of a Cell as a char (tsdb::char_t)</summary>
 * <remarks>FieldTypes of CHAR only can be converted to char_t.  Others throw type_conversion_error.</remarks>
 */
tsdb::char_t Cell::toChar() const {
	switch(this->my_field_type) {
		case tsdb::Field::CHAR:
			return *((tsdb::char_t*) this->my_memory_block_ptr.raw());
			break;
		default:
			throw tsdb::type_conversion_error("cannot convert type to char");
			break;
	}
}

/** <summary>Returns the value of a Cell as a timestamp (tsdb::timestamp_t)</summary>
 * <remarks>FieldTypes of TIMESTAMP and DATE can be converted to timestamp_t. 
 * Dates are converted so that the timestamp is at 00:00 hours.
 * Others throw type_conversion_error.</remarks>
 */
tsdb::timestamp_t Cell::toTimestamp() const {
	tsdb::date_t tmp_date;
	switch(this->my_field_type) {
		case tsdb::Field::TIMESTAMP:
			return *((tsdb::timestamp_t*) this->my_memory_block_ptr.raw());
			break;
		case tsdb::Field::DATE:
			tmp_date = *((tsdb::date_t*) this->my_memory_block_ptr.raw());
			tsdb::timestamp_t tmp_timestamp;
			tmp_timestamp = tmp_date * 86400 * 1000;
			return tmp_timestamp;
			break;
		default:
			throw tsdb::type_conversion_error("cannot convert type to timestamp");
			break;
	}
}

/** <summary>Returns the value of a Cell as a record_id (tsdb::record_id)</summary>
 * <remarks>FieldTypes of RECORD only can be converted to record_t.  Others throw type_conversion_error.</remarks>
 */
tsdb::record_t Cell::toRecordId() const {
	switch(this->my_field_type) {
		case tsdb::Field::RECORD:
			return *((tsdb::record_t*) this->my_memory_block_ptr.raw());
			break;
		default:
			throw tsdb::type_conversion_error("cannot convert type to recordid");
			break;
	}
}

/** <summary>Returns the value of a Cell as a date (tsdb::date_t)</summary>
 * <remarks>FieldTypes of DATE only can be converted to date_t.  Others throw type_conversion_error.</remarks>
 */
tsdb::date_t Cell::toDate() const {
	switch(this->my_field_type) {
		case tsdb::Field::DATE:
			return *((tsdb::date_t*) this->my_memory_block_ptr.raw());
			break;
		default:
			throw tsdb::type_conversion_error("cannot convert type to date");
			break;
	}
}

/** <summary>Sets a cell equal to a double</summary>
 * <remarks>FieldTypes of DOUBLE and INT32 support double assignment.
 * Others throw type_conversion_error. If the FieldType of the cell is INT32,
 * assignment to a double drops the fractional part, and will throw a type_conversion_error
 * if the abs(rhs) > 2147483647.</remarks>
 */
tsdb::Cell& Cell::operator=(const tsdb::ieee64_t &rhs) {
	switch(this->my_field_type) {
		case tsdb::Field::DOUBLE:
			memcpy(this->my_memory_block_ptr.raw(),&rhs,sizeof(tsdb::ieee64_t));
			break;
		case tsdb::Field::INT32:
			// need to check the bounds first
			if(rhs > 2147483647.0 || rhs < -2147483647.0) {
				throw tsdb::type_conversion_error("double out of bounds for conversion to int32");
			} else {
				tsdb::int32_t temp_int32;
				temp_int32 = (tsdb::int32_t) rhs; // note: we just loose the fractional part, no rounding
				memcpy(this->my_memory_block_ptr.raw(),&temp_int32,sizeof(tsdb::int32_t));
			}
			break;
		case tsdb::Field::INT8:
			// need to check the bounds first
			if(rhs > 127.0 || rhs < -127.0) {
				throw tsdb::type_conversion_error("double out of bounds for conversion to int32");
			} else {
				tsdb::int8_t temp_int8;
				temp_int8 = (tsdb::int8_t) rhs; // note: we just loose the fractional part, no rounding
				memcpy(this->my_memory_block_ptr.raw(),&temp_int8,sizeof(tsdb::int8_t));
			}
			break;
		default:
			throw tsdb::type_conversion_error("the cell's field type does not support assignment from double");
			break;
	}
	return *this;
}


/** <summary>Sets a cell equal to a signed 64-bit integer</summary>
 * <remarks>FieldTypes of TIMESTAMP only support assignment of signed 64-bit integers.
 * Others throw type_conversion_error.</remarks>
 */
tsdb::Cell& Cell::operator=(const tsdb::int64_t &rhs) {
	switch(this->my_field_type) {
		case tsdb::Field::TIMESTAMP:
			memcpy(this->my_memory_block_ptr.raw(),&rhs,sizeof(tsdb::int64_t));
			break;
		default:
			throw tsdb::type_conversion_error("the cell's field type does not support assignment from int64");
			break;
	}
	return *this;
}

/** <summary>Sets a cell equal to a signed 8-bit integer</summary>
 * <remarks>FieldTypes of INT8, INT32, DOUBLE, and CHAR support assignment of signed 8-bit integers.
 * For CHAR is not guaranteed to be signed on all systems, so when cell FieldType is CHAR and rhs < 0,
 * the bit sequence of the number is preserved but not necessairly the signedness.
 * Others cell FieldTypes throw type_conversion_error.</remarks>
 */
tsdb::Cell& Cell::operator=(const tsdb::int8_t &rhs) {
	switch(this->my_field_type) {
		case tsdb::Field::INT8:
			memcpy(this->my_memory_block_ptr.raw(),&rhs,sizeof(tsdb::int8_t));
			break;
		case tsdb::Field::INT32:
			tsdb::int32_t temp_int32;
			temp_int32 = (tsdb::int32_t) rhs;
			memcpy(this->my_memory_block_ptr.raw(),&temp_int32,sizeof(tsdb::int32_t));
			break;
		case tsdb::Field::DOUBLE:
			tsdb::ieee64_t temp_double;
			temp_double = (tsdb::ieee64_t) rhs;
			memcpy(this->my_memory_block_ptr.raw(),&temp_double,sizeof(tsdb::ieee64_t));
			break;
		/* Allow this method to be called on my_field_type == CHAR
		 * because in most cases, an int8 is indistinguishable to a char. */
		case tsdb::Field::CHAR: 
			tsdb::char_t temp_char;
			temp_char = (tsdb::char_t) rhs;
			memcpy(this->my_memory_block_ptr.raw(),&temp_char,sizeof(tsdb::char_t));
			break;
		default:
			throw tsdb::type_conversion_error("the cell's field type does not support assignment from int8");
			break;
	}
	return *this;
}

/** <summary>Sets a cell equal to a char</summary>
 * <remarks>FieldTypes of CHAR only support assignment of chars.
 * Others throw type_conversion_error.</remarks>
 */
tsdb::Cell& Cell::operator=(const tsdb::char_t &rhs) {
	switch(this->my_field_type) {
		case tsdb::Field::CHAR: 
			tsdb::char_t temp_char;
			temp_char = (tsdb::char_t) rhs;
			memcpy(this->my_memory_block_ptr.raw(),&temp_char,sizeof(tsdb::char_t));
			break;
		default:
			throw tsdb::type_conversion_error("the cell's field type does not support conversion from int8");
			break;
	}
	return *this;
}

/** <summary>Sets a cell equal to a signed 32-bit integer</summary>
 * <remarks>FieldTypes of INT32, INT8, DOUBLE, TIMESTAMP, and DATE support
 *  assignment of signed 32-bit integers. For cells with field types of TIMESTAMP,
 *  rhs is treated as a date number (meaning the number of days since Jan 1, 1970), 
 * then converted to a timestamp at 00:00 hours on that day.
 * Others field types throw type_conversion_error.</remarks>
 */
tsdb::Cell& Cell::operator=(const tsdb::int32_t &rhs) {
	switch(this->my_field_type) {
		case tsdb::Field::INT32:
			memcpy(this->my_memory_block_ptr.raw(),&rhs,sizeof(tsdb::int32_t));
			break;
		case tsdb::Field::INT8:
			tsdb::int8_t temp_int8;
			if(rhs > 127 || rhs < 127) {
				throw tsdb::type_conversion_error("int32 out of bounds for conversion to int8");
			} else {
				temp_int8 = (tsdb::int8_t) rhs;
				memcpy(this->my_memory_block_ptr.raw(),&temp_int8,sizeof(tsdb::int8_t));
			}
			break;
		case tsdb::Field::DATE:
			memcpy(this->my_memory_block_ptr.raw(),&rhs,sizeof(tsdb::int32_t));
			break;
		case tsdb::Field::TIMESTAMP:
			tsdb::int64_t temp_ts;
			temp_ts = rhs * 86400 * 1000;
			memcpy(this->my_memory_block_ptr.raw(),&temp_ts,sizeof(tsdb::int64_t));
		case tsdb::Field::DOUBLE:
			tsdb::ieee64_t temp_double;
			temp_double = (tsdb::ieee64_t) rhs;
			memcpy(this->my_memory_block_ptr.raw(),&temp_double,sizeof(tsdb::ieee64_t));
			break;
		default:
			throw tsdb::type_conversion_error("the cell's field type does not support conversion from int8");
			break;
	}
	return *this;
}

/** <summary>Sets a cell equal to a unsigned 64-bit integer</summary>
 * <remarks>FieldTypes of RECORD only support assignment of unsigned 64-bit integers.
 * Others throw type_conversion_error.</remarks>
 */
tsdb::Cell& Cell::operator=(const tsdb::uint64_t &rhs) {
	switch(this->my_field_type) {
		case tsdb::Field::RECORD:
			memcpy(this->my_memory_block_ptr.raw(),&rhs,sizeof(tsdb::uint64_t));
			break;
		default:
			throw tsdb::type_conversion_error("the cell's field type does not support conversion from recordid");
			break;
	}
	return *this;
}



tsdb::Field::FieldType Cell::fieldType() const {
	return this->my_field_type;
}

/** <summary>Parses a string and sets it as the cell's payload</summary>
 * <remarks>This works for field types of CHAR, DOUBLE, INT8, and INT32.
 * For char, the first character of the string is used. If the string is empty,
 * then the cell is set to '\0'. For DOUBLE, INT32, and INT8, no range checking
 * exists at this time. Numbers out of range for these datatypes have undefined
 * behaivor.</remarks>
 */
tsdb::Cell& Cell::operator=(const std::string &rhs) {

	int tmp_int;
	switch(this->my_field_type) {
		case tsdb::Field::CHAR:
			tsdb::char_t tmp_char;
			if(rhs.length() > 0) {
				tmp_char = (tsdb::char_t) rhs.c_str()[0]; // use the first character
			} else {
				tmp_char = '\0';
			}
			memcpy(this->my_memory_block_ptr.raw(),&tmp_char,sizeof(tsdb::char_t));
			break;
		case tsdb::Field::DOUBLE:
			tsdb::ieee64_t tmp_dbl;
			tmp_dbl = (tsdb::ieee64_t) atof(rhs.c_str());
			memcpy(this->my_memory_block_ptr.raw(),&tmp_dbl,sizeof(tsdb::ieee64_t));
			break;
		case tsdb::Field::INT8:
			tsdb::int8_t tmp_int8;
			tmp_int = atoi(rhs.c_str());
			tmp_int8 = (tsdb::int8_t) tmp_int;
			// TODO: add some bounds checking here
			memcpy(this->my_memory_block_ptr.raw(),&tmp_int8,sizeof(tsdb::int8_t));
			break;
		case tsdb::Field::INT32:
			tsdb::int32_t tmp_int32;
			tmp_int = atoi(rhs.c_str());
			tmp_int32 = (tsdb::int32_t) tmp_int;
			// TODO: add some bounds checking here
			memcpy(this->my_memory_block_ptr.raw(),&tmp_int32,sizeof(tsdb::int32_t));
			break;
		case tsdb::Field::STRING:
			memset(my_memory_block_ptr.raw(),0,my_size);
			memcpy(my_memory_block_ptr.raw(),rhs.c_str(),
				(rhs.length() > my_size ? my_size : rhs.length()));
			break;
		default:
			throw tsdb::type_conversion_error("cannot convert string to this cell's field type");
			break;
	}
	return *this;
}



/** <summary>Streams a string representation of Cell</summary>
 * <remarks>Streams a string representation of a Cell, so you can stream it to
 * cout and the like. Example:
 * \code
 * cout << myCell << endl;
 * \endcode
 * </remarks>
 */
std::ostream& operator<<(std::ostream &stream, const tsdb::Cell& ob)
{
	stream << ob.toString();

	return stream;
}


} //namespace tsdb

