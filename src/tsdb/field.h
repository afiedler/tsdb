#pragma once

/* STL Classes */
#include <string>

/* External Libraries */
#include "hdf5.h"


#include "tsdb.h"



namespace tsdb {



/* Explicitly define binary types */
typedef long int32_t;
typedef double ieee64_t;
typedef signed char int8_t;
typedef char char_t;
typedef long long timestamp_t;
typedef long long int64_t;
typedef unsigned long long uint64_t;
typedef unsigned long long record_t;
typedef long date_t;

/* Forward declarations */

/* -----------------------------------------------------------------
 * Base Field class. This class is not created in practice.
 * -----------------------------------------------------------------
 */

/** <summary>Base Field class. Not used in practice.</summary> 
 * <remarks>The Field classes represent a datatype in TSDB, such as integers, timestamps, etc. A Field
 * object has a number of methods that parse datatypes out of strings, convert back to strings, and 
 * interface with the unsderlying HDF5 library. </remarks>
 */
class  Field
{
public:
	enum FieldType {
		INT32,
		INT8,
		DOUBLE,
		CHAR,
		RECORD,
		TIMESTAMP,
		DATE,
		STRING,
		UNDEFINED
	};

	const size_t getSizeOf(void);
	const hid_t getHDF5Type(void);
	const std::string getName(void);
	const FieldType getFieldType(void);
	virtual const std::string toString(const void* fld);
	virtual const std::string getTSDBType();
	~Field(void);

protected:
	
	/* Use a protected constructor. A Field should not be created, instead
	 * create one of the Field subclasses
	 */
	Field(std::string new_name, hid_t new_type, size_t new_size_of,
		FieldType new_field_type);
	
	/* Protected properties */
	size_t size_of;
	hid_t hdf5_type;
	std::string name;
	tsdb::Field::FieldType field_type;
};


/* -----------------------------------------------------------------
 * TimestampField class. This class represents a 64-bit timestamp
 * -----------------------------------------------------------------
 */

/** <summary>Represents a 64-bit signed integer timestamp</summary>
 * <remarks>Timestamps are stored in TSDB as 64-bit signed integers. The epoch is 1-Jan-1970 00:00:00.
 * The least signifigant bit of the timestamp represents one millisecond, so a timestamp is effectively
 * a standard POSIX timestamp times 1,000. Integer timestamps are used to eliminate the inaccuracites of
 * floating-point comparisons.</remarks>
 */
class  TimestampField: public Field
{
public:

	TimestampField(std::string new_name);
	const std::string toString(const void* fld);
	const std::string getTSDBType();

};

/* -----------------------------------------------------------------
 * DateField class. This class represents a 32-bit date number
 * -----------------------------------------------------------------
 */

/** <summary>Represents a 32-bit signed integer date number.</summary>
 * <remarks>The epoch for dates is defined as 1-Jan-2000 = 0.</remarks>
 */
class  DateField: public Field
{
public:

	DateField(std::string new_name);
	const std::string toString(const void* fld);
	const std::string getTSDBType();

};



/* -----------------------------------------------------------------
 * RecordField class. This class represents a record index number
 * -----------------------------------------------------------------
 */
/** <summary>Represents a 64-bit unsigned integer record id</summary>
 * <remarks>The HDF5 library has 64-bit record identifiers, and they are used in TSDB directly.
 * This field type should be used for fields that specifically refer to other records by ID number.</remarks>
 */
class  RecordField: public Field
{
public:

	RecordField(std::string new_name);
	const std::string toString(const void* fld);
	const std::string getTSDBType();

};



/* -----------------------------------------------------------------
 * Int32Field class. Represents a signed 32-bit integer
 * -----------------------------------------------------------------
 */

/** <summary>Represents a 32-bit signed integer.</summary>
 * <remarks>This represents an <c>int</c> type on most systems. Note that it is not 
 * currently possible to store missing values in Int32 fields. You should consider a 
 * DoubleField if you need missing value support.</remarks>
 */
class  Int32Field: public Field
{
public:

	Int32Field(std::string new_name);
	const std::string toString(const void* fld);
	const std::string getTSDBType();

};

/* -----------------------------------------------------------------
 * Int8Field class. Represents a signed 8-bit integer
 * -----------------------------------------------------------------
 */

/** <summary>Represents a 8-bit signed integer.</summary>
 * <remarks>This represents an <c>signed char</c> type on most systems. Note that it is not 
 * currently possible to store missing values in Int8 fields. You should consider a 
 * DoubleField if you need missing value support.</remarks>
 */
class  Int8Field: public Field
{
public:

	Int8Field(std::string new_name);
	const std::string toString(const void* fld);
	const std::string getTSDBType();

};


/* -----------------------------------------------------------------
 * DoubleField class. Represents a 64-bit IEEE floating point number
 * -----------------------------------------------------------------
 */

/** <summary>Represents a 64-bit IEEE floating point number</summary>
 * <remarks>This represents an <c>double</c> type on most systems. IEEE floating point
 numbers are able to store a number of special values, such as infinity, underflows, overflows,
 and NaNs Missing values are saved in TSDB as IEEE quiet NaNs.</remarks>
 */
class  DoubleField: public Field
{
public:

	DoubleField(std::string new_name);
	const std::string toString(const void* fld);
	const std::string getTSDBType();

};


/* -----------------------------------------------------------------
 * CharField class. Represents a 8-bit character
 * -----------------------------------------------------------------
 */

/** <summary>Represents a 8-bit character</summary>
 * <remarks>This represents an <c>char</c> type.</remarks>
 */
class  CharField: public Field
{
public:

	CharField(std::string new_name);
	const std::string toString(const void* fld);
	const std::string getTSDBType();

};

/* -----------------------------------------------------------------
 * String class. Represents a string
 * -----------------------------------------------------------------
 */

/** <summary>Represents a string</summary>
 * <remarks>This is a variable length field.</remarks>
 */
class  StringField: public Field
{
public:

	StringField(std::string new_name, int _length);
	const std::string toString(const void* fld);
	const std::string getTSDBType();

};

} // namespace tsdb

	
