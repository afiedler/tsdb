/* STL Classes */
#include <string>
#include <sstream>
#include "string.h"

/* External Libraries */
#include "boost/date_time/posix_time/posix_time.hpp"
#include "hdf5.h"

/* TSDB Includes */
#include "field.h"
#include "tsdb.h"

using namespace std;

namespace tsdb {

/* ====================================================================
 * class Field - Field base class.
 * ====================================================================
 */

/**
 * <summary>
 * Unused. Create a subclass of Field instead.
 * </summary>
 * <param name="new_name">A name for the Field</param>
 * <param name="new_type">HDF5 Type of the Field</param>
 * <param name="new_size_of">Size of the Field, in bytes</param>
 */
Field::Field(std::string new_name, hid_t new_type, 
			 size_t new_size_of, FieldType new_field_type){
	this->name = new_name;
	this->hdf5_type = new_type;
	this->size_of = new_size_of;
	this->field_type = new_field_type;
}

/**
 * <summary>
 * Gets the name of a Field.
 * </summary>
 */
const string Field::getName(void) {
	return this->name;
}

/**
 * <summary>
 * Gets the size of a Field. This would be equivalent to the <c>sizeof</c> operator.
 * </summary>
 */
const size_t Field::getSizeOf(void) {
	return this->size_of;
}

/**
 * <summary>
 * Gets the HDF5 type of a Field.
 * </summary>
 */
const hid_t Field::getHDF5Type(void) {
	return this->hdf5_type;
}

/**
 * <summary>
 * Gets the FieldType of the field.
 * </summary>
 */
const tsdb::Field::FieldType Field::getFieldType(void) {
	return this->field_type;
}

/**
 * <summary>
 * Destructor.
 * </summary>
 */
Field::~Field(void)
{
}

/**
 * <summary>
 * Returns a string representation of a Field. This class is overloaded in subclasses.
 * </summary>
 * <param name="fld">Pointer to a memory block containing data to be represented as a
 * string</param>
 */
const string Field::toString(const void *fld) {
	return "";
}

/**
 * <summary>
 * Returns a string representing the TSDB type of the field. 
 * </summary>
 * <remarks> The type string corresponds to the 
 * class name without the word <c>Field</c>. For this base class, or for any class that
 * does not properly overload
 * this, this function will return "undefined".
 * </remarks>
 */
const string Field::getTSDBType() {
	return "Undefined";
}

/* ====================================================================
 * class TimestampField 
 * ====================================================================
 */

/**
 * <summary>
 * Creates a new Timestamp field.
 * </summary>
 * <param name="new_name">A name for this Field.</param>
 */
TimestampField::TimestampField(std::string new_name):
Field(new_name, H5T_NATIVE_LLONG, sizeof(tsdb::timestamp_t),tsdb::Field::TIMESTAMP) {}

/**
 * <summary>
 * Returns a string representing the timestamp.
 * </summary>
 * <remarks>This method returns a ISO
 * timestamp with the format: YYYY-MM-DDThh:mm:ss.ffffff
 * </remarks>
 * <param name="fld">A pointer to a memory block containing a <c>timestamp_t</c>
 * which will be converted to a string.</param>
 */
const string TimestampField::toString(const void* fld) {
	using namespace boost::gregorian;
	using namespace boost::posix_time;

	timestamp_t timestamp;
	timestamp = *((timestamp_t*) fld);
	
	ptime epoch(date(1970,1,1));
	/* NOTE: This will overflow at some point, likely year 70021, at which point I
	 * won't worry about it! */
	ptime thistime = epoch + seconds((long)(timestamp / 1000)) + milliseconds((long)(timestamp % 1000));
	//standardizing output
	string timeString = to_iso_extended_string(thistime);
	if (timeString.length()==19) timeString = timeString + ".000";
	else if (timeString.length()==26) timeString = timeString.substr(0,23);
	return timeString;
}

/**
 * <summary>
 * Returns the TSDB type of the Field. For TimestampField, this is "Timestamp"
 * </summary>
 */
const string TimestampField::getTSDBType() {
	return "Timestamp";
}

/* ====================================================================
 * class DateField 
 * ====================================================================
 */

/**
 * <summary>
 * Creates a new Date field.
 * </summary>
 * <param name="new_name">A name for this Field.</param>
 */
DateField::DateField(std::string new_name):
Field(new_name, H5T_NATIVE_LONG, sizeof(tsdb::date_t),tsdb::Field::DATE) {}

/**
 * <summary>
 * Returns a string representing the date.
 * </summary>
 * <remarks>This method returns a ISO
 * date with the format: YYYY-MM-DD
 * </remarks>
 * <param name="fld">A pointer to a memory block containing a <c>date_t</c>
 * which will be converted to a string.</param>
 */
const string DateField::toString(const void* fld) {

	tsdb::date_t days;
	days = *((tsdb::date_t*) fld);
	
	boost::gregorian::date epoch(1970,1,1);
	boost::gregorian::date this_day;
	this_day = epoch + boost::gregorian::date_duration(days);

	return to_iso_extended_string(this_day);
}

/**
 * <summary>
 * Returns the TSDB type of the Field. For DateField, this is "Date"
 * </summary>
 */
const string DateField::getTSDBType() {
	return "Date";
}

/* ====================================================================
 * class RecordField 
 * ====================================================================
 */

/**
 * <summary>
 * Creates a new record field.
 * </summary>
 * <param name="new_name">A name for this Field.</param>
 */
RecordField::RecordField(std::string new_name):
Field(new_name, H5T_NATIVE_ULLONG, sizeof(tsdb::record_t), tsdb::Field::RECORD) {}

/**
 * <summary>
 * Returns the TSDB type of the Field.
 * </summary>
 * <remarks>
 * For RecordField, this is "Record".
 * </remarks>
 */
const string RecordField::getTSDBType() {
	return "Record";
}

/**
 * <summary>
 * Returns a string representing the record.
 * </summary>
 * <remarks>
 *  The record is returned as a decimal number.
 * </remarks>
 * <param name="fld">A pointer to a memory block containing a <c>hsize_t</c> record number
 * which will be converted to a string.</param>
 */
const string RecordField::toString(const void* fld) {
	hsize_t recid;
	recid = *((hsize_t*) fld);

	stringstream sout;
	sout << recid;
	return sout.str();
}


/* ====================================================================
 * class Int32Field 
 * ====================================================================
 */

/**
 * <summary>
 * Creates a new 32-bit integer field.
 * </summary>
 * <param name="new_name">A name for this Field.</param>
 */
Int32Field::Int32Field(std::string new_name):
Field(new_name, H5T_NATIVE_LONG, sizeof(tsdb::int32_t), tsdb::Field::INT32) {}

/**
 * <summary>
 * Returns the TSDB type of the Field. 
 * </summary>
 * <remarks>
 * For Int32Field, this is "Int32".
 * </remarks>
 */
const string Int32Field::getTSDBType() {
	return "Int32";
}

/**
 * <summary>
 * Returns a string representing the integer as a decimal number.
 * </summary>
 * <param name="fld">A pointer to a memory block containing 32-bit signed integer
 * which will be converted to a string.</param>
 */
const string Int32Field::toString(const void* fld) {
	int32_t fldint;
	fldint = *((int32_t*) fld);

	stringstream sout;
	sout << fldint;
	return sout.str();
}

/* ====================================================================
 * class Int8Field 
 * ====================================================================
 */

/**
 * <summary>
 * Creates a new 8-bit integer field.
 * </summary>
 * <param name="new_name">A name for this Field.</param>
 */
Int8Field::Int8Field(std::string new_name):
Field(new_name, H5T_NATIVE_CHAR, sizeof(tsdb::int8_t), tsdb::Field::INT8) {}

/**
 * <summary>
 * Returns the TSDB type of the Field. 
 * </summary>
 * <remarks>
 * For Int32Field, this is "Int32".
 * </remarks>
 */
const string Int8Field::getTSDBType() {
	return "Int8";
}

/**
 * <summary>
 * Returns a string representing the integer as a decimal number.
 * </summary>
 * <param name="fld">A pointer to a memory block containing 32-bit signed integer
 * which will be converted to a string.</param>
 */
const string Int8Field::toString(const void* fld) {
	int8_t fldint;
	int integer;
	fldint = *((int8_t*) fld);
	integer = (int) fldint;
	stringstream sout;
	sout << integer;
	return sout.str();
}

/* ====================================================================
 * class DoubleField 
 * ====================================================================
 */

/**
 * <summary>
 * Creates a new IEEE 64-bit floating point field.
 * </summary>
 * <param name="new_name">A name for this Field.</param>
 */
DoubleField::DoubleField(std::string new_name):
Field(new_name, H5T_NATIVE_DOUBLE, sizeof(tsdb::ieee64_t), tsdb::Field::DOUBLE) {}

/**
 * <summary>
 * Returns the TSDB type of the Field.
 * </summary>
 * <remarks>
 * For DoubleField, this is "Double".
 * </remarks>
 */
const string DoubleField::getTSDBType() {
	return "Double";
}

/**
 * <summary>
 * Returns a string representing the double. 
 * </summary>
 * <remarks>
 * The double is returned using standard 
 * floating-point representation.
 * </remarks>
 * <param name="fld">A pointer to a memory block containing 64-bit IEEE floating point
 * number which will be converted to a string.</param>
 */
const string DoubleField::toString(const void* fld) {
	ieee64_t doublefld;
	doublefld = *((ieee64_t*) fld);

	stringstream sout;
	sout << doublefld;
	return sout.str();
}

/* ====================================================================
 * class CharField 
 * ====================================================================
 */

/**
 * <summary>
 * Creates a new char field. 
 * </summary>
 * <remarks>
 * A Char field represents just one 8-bit character. In future releases, this will support strings
 * of characters.
 * </remarks>
 * <param name="new_name">A name for this Field.</param>
 */
CharField::CharField(std::string new_name):
Field(new_name, H5T_NATIVE_CHAR, sizeof(tsdb::char_t), tsdb::Field::CHAR) {
}

/**
 * <summary>
 * Returns the TSDB type of the Field. 
 * </summary>
 * <remarks>
 * For CharField, this is "Char"
 * </remarks>
 */
const string CharField::getTSDBType() {
	return "Char";
}

/**
 * <summary>
 * Returns a string representing the char.
 * </summary>
 * <param name="fld">A pointer to a memory block containing a char
 * which will be converted to a string.</param>
 */
const string CharField::toString(const void* fld) {
	char charfld;
	charfld = *((char*) fld);

	stringstream sout;
	sout << charfld;
	return sout.str();
}


/* ====================================================================
 * class StringField 
 * ====================================================================
 */

/**
 * <summary>
 * Creates a new string field. 
 * </summary>
 * <remarks>
 * </remarks>
 * <param name="new_name">A name for this Field.</param>
 */
StringField::StringField(std::string new_name, int _length):
Field(new_name, 0, 0, tsdb::Field::STRING) {
	hid_t strtype;
	herr_t status;
	strtype = H5Tcopy(H5T_C_S1);
	status = H5Tset_size(strtype,_length); // TODO: check for error. Garbage collect this.
	this->hdf5_type = strtype;
	this->size_of = _length;
}

/**
 * <summary>
 * Returns the TSDB type of the Field. 
 * </summary>
 * <remarks>
 * For StringField, this is "String(length)"
 * </remarks>
 */
const string StringField::getTSDBType() {
	std::stringstream ss;
	ss << "String(" << this->size_of << ")";
	return ss.str();
}

/**
 * <summary>
 * Returns a string representing the char.
 * </summary>
 * <param name="fld">A pointer to a memory block containing a char
 * which will be converted to a string.</param>
 */
const string StringField::toString(const void* fld) {
	char * fld_buf = (char*) malloc(this->size_of+1);
	memset(fld_buf,0,this->size_of+1);
#ifdef WIN32
	strcpy_s(fld_buf,this->size_of,(char*)fld);
#else
    strcpy(fld_buf,(char*)fld);
#endif

	stringstream sout;
	sout << fld_buf;
	return sout.str();
}

} // namespace tsdb