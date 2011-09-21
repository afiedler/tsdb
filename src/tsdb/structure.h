#pragma once

/* STL Classes */
#include <string>
#include <vector>

/* External Libraries */
#include "hdf5.h"

/* TSDB Includes */
#include "field.h"
#include "tsdb.h"

#ifdef _GCC_
#include <stdexcept>
#include <cstring>
#endif

namespace tsdb {
/* Forward Declarations */

class Record;

/* -----------------------------------------------------------------
 * StructureException. For runtime errors thrown by the Structure 
 * class.
 * -----------------------------------------------------------------
 */
class  StructureException: 
	public std::runtime_error 
{ 
public:

	StructureException(const std::string& what):
	  std::runtime_error(std::string("StructureException: ") + what) {} 

};

/* -----------------------------------------------------------------
 * Structure. Represents a dynamically-defined structure
 * -----------------------------------------------------------------
 */
class  Structure
{
public:
	/* Constructors */
	Structure(std::vector<Field*> new_fields, bool align_memory);
	Structure(std::vector<Field*> new_fields, std::vector<size_t> offsets,
		size_t struct_size);

	/* Methods to get information about the Structure */
	size_t getSizeOf(void);
	size_t getNFields(void);
	size_t* getSizeOfFieldsAsArray(void);
	char** getNameOfFieldsAsArray(void);
	size_t* getOffsetOfFieldsAsArray(void);
	hid_t* getTypeOfFieldsAsArray(void);
	size_t getOffsetOfField(size_t n);
	size_t getSizeOfField(size_t n);
	Field* getField(size_t n);
	size_t getFieldIndexByName(std::string field_name);

	/* Methods to access a block of memory using the Structure definition */
	void* pointerToMember(const void *struct_start, size_t ifield);
	void* pointerToMember(const void *struct_start, size_t irecord, size_t ifield);
	void setMember(void *struct_start, size_t ifield, const void *src);
	void setMember(void *struct_start, size_t irecord, size_t ifield, const void *src);

	/* Methods to convert the Structure to other datatypes */
	std::string structsToString(void* structptr, size_t nrecords, std::string field_delim,
		std::string record_delim);

	/* Destructor */
	~Structure(void);

private:
	/* Properties */
	std::vector<Field*> fields;
	size_t size_of;
	size_t nfields;
	size_t* size_of_fields;
	size_t* offset_of_fields;
	hid_t* type_of_fields;
	char** name_of_fields;
};

} // namespace tsdb
