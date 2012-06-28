#include "structure.h"
#include "record.h"
#include "tsdb.h"
#include <vector>


using namespace std;

namespace tsdb {

/**
 * <summary>
 * Creates a new Structure object from a vector of Fields.
 * </summary>
 * <remarks>
 * A Structure is analogous to a C struct, except
 * here it can be dynamically defined.</remarks>
 * <param name="new_fields">A vector of Field pointers</param>
 * <param name="align_memory"><c>true</c> and the offsets of the fields will be aligned to
 * the architecture's word size (as defined in <c>ARCH_WORD_SIZE</c>). <c>false</c> will have <c>Fields</c>
 * tightly packed, which may cause pointer mis-alignment errors and/or slower memory 
 * access on some systems.</param>
 */
Structure::Structure(std::vector<Field*> new_fields, bool align_memory) {
	this->fields = new_fields;
	this->nfields = this->fields.size();
	this->size_of_fields = (size_t*) malloc(sizeof(size_t)*this->nfields);
	this->type_of_fields = (hid_t*) malloc(sizeof(hid_t)*this->nfields);
	this->offset_of_fields = (size_t*) malloc(sizeof(size_t)*this->nfields);
	this->name_of_fields = (char**) malloc(sizeof(char*)*this->nfields);

	
	size_t offset = 0;
	for(size_t i=0;i < this->fields.size();i++) {
		this->size_of_fields[i] = this->fields[i]->getSizeOf();
		this->type_of_fields[i] = this->fields[i]->getHDF5Type();
		this->offset_of_fields[i] = offset;
		offset += this->size_of_fields[i];
		if(align_memory) {
			// Round offset up to an even multiple of ARCH_WORD_SIZE
			offset = (offset % ARCH_WORD_SIZE == 0) ? offset : offset + (ARCH_WORD_SIZE - (offset % ARCH_WORD_SIZE));
		}
		this->name_of_fields[i] = (char*) malloc(sizeof(char)*
			this->fields[i]->getName().length()+1);
		strcpy(this->name_of_fields[i],this->fields[i]->getName().c_str());
	}
	this->size_of = offset;
}
/**
 * <summary>
 * Creates a new Structure object from a vector of Fields and offsets.
 * </summary>
 * <remarks> A Structure is analogous to a C struct, except
 * this object allows it to be defined at runtime. This constructor is for use when
 * you have manually specified the offsets of the Fields. You might do this when, for example,
 * you want to exactly mirror a compile-time defined C struct with a Structure object.
 * </remarks>
 * <param name="new_fields">A vector of Field pointers</param>
 * <param name="offsets">A vector of Field offsets, such as from the <c>offsetof()</c> macro</param>
 * <param name="struct_size">The number of bytes occupied by the whole structure. Typically you
 * would use the <c>sizeof()</c> operator here.</param>
 */
Structure::Structure(std::vector<Field*> new_fields, std::vector<size_t> offsets, size_t struct_size) {
	this->fields = new_fields;
	this->nfields = this->fields.size();
	this->size_of_fields = (size_t*) malloc(sizeof(size_t)*this->nfields);
	this->type_of_fields = (hid_t*) malloc(sizeof(hid_t)*this->nfields);
	this->offset_of_fields = (size_t*) malloc(sizeof(size_t)*this->nfields);
	this->name_of_fields = (char**) malloc(sizeof(char*)*this->nfields);

	
	for(size_t i=0;i < this->fields.size();i++) {
		this->size_of_fields[i] = this->fields[i]->getSizeOf();
		this->type_of_fields[i] = this->fields[i]->getHDF5Type();
		this->offset_of_fields[i] = offsets.at(i);
		this->name_of_fields[i] = (char*) malloc(sizeof(char)*
			this->fields[i]->getName().length()+1);
		strcpy(this->name_of_fields[i],this->fields[i]->getName().c_str());
	}
	this->size_of = struct_size;
}

/** <summary>Destructor</summary>
 * <remarks>
 * This destructor deletes all linked Fields.
 * </remarks>
 */
Structure::~Structure(void) {
	free(this->size_of_fields);
	free(this->offset_of_fields);
	free(this->type_of_fields);
	size_t i;
	for(i=0;i<this->nfields;i++) {
		free(this->name_of_fields[i]);
	}
	
	free(this->name_of_fields);

	for(i=0;i<this->nfields;i++) {
		delete this->fields.at(i);
	}

}

/** <summary>Returns a pointer to an array of pointers to C strings, each containing the name of a field.</summary>
 */
char** Structure::getNameOfFieldsAsArray(void) {
	return this->name_of_fields;
}

/** <summary>Returns a pointer to an array size_t integers, each containing the size of a field.</summary>
 */
size_t* Structure::getSizeOfFieldsAsArray(void) {
	return this->size_of_fields;
}

/** <summary>Returns a size_t integer with the size in bytes of the Structure.</summary>
 * <remarks>This is equivalent to the <c>sizeof()</c> operator.</remarks>
 */
size_t Structure::getSizeOf(void) {
	return this->size_of;
}

/** <summary>Returns a size_t integer with the size in bytes of the a field.</summary>
 * <remarks>This is equivalent to the <c>sizeof()</c> operator on a <c>struct</c> member.</remarks>
 * <param name="n">Field index</param>
 */
size_t Structure::getSizeOfField(size_t n) {
	return this->size_of_fields[n];
}

/** <summary>Returns a pointer to an array size_t integers, each containing the offset of a field.</summary>
 * <remarks>The offsets returned here are equivalent to offsets returned by the <c>offsetof()</c> macro.</remarks>
 */
size_t* Structure::getOffsetOfFieldsAsArray(void) {
	return this->offset_of_fields;
}

/** <summary>Returns a pointer to an array hid_t integers, each containing the Hd5F type of a field.</summary>
 */
hid_t* Structure::getTypeOfFieldsAsArray(void) {
	return this->type_of_fields;
}

/** <summary>Returns a pointer to a Field at index <c>n</c>.</summary> */
Field* Structure::getField(size_t n) {
	return this->fields.at(n);
}

/** <summary>Returns a size_t integer offset of the field at index <c>n</c>.</summary> */
size_t Structure::getOffsetOfField(size_t n) {
	return this->offset_of_fields[n];
}

/** <summary>Returns the number of fields in the Structure</summary> */
size_t Structure::getNFields(void) {
	return this->nfields;
}

/** <summary>Returns a pointer to field member.</summary>
 * <remarks>This function returns a pointer to the start of a specific field. You must supply it
 * with a pointer to the start of the structure. You would not use this function on an array of
 * structures, instead you would use Structure::pointerToMember(const void *struct_start, size_t irecord, size_t ifield).
 * </remarks>
 * <param name="struct_start">Pointer to the start of a structure.</param>
 * <param name="ifield">Which field you want, counting from zero.</param>
 */
void* Structure::pointerToMember(const void *struct_start, size_t ifield) {
	return (void*)((char*) struct_start + getOffsetOfField(ifield));
}

/** <summary>Returns a pointer to field member in an array of structures.</summary>
 * <remarks>This function returns a pointer to the start of a specific field. You must supply it
 * with a pointer to the start of a structure array.</remarks>
 * <param name="struct_start">Pointer to the start of a structure.</param>
 * <param name="irecord">Which structure you want in the array, counting from zero.</param>
 * <param name="ifield">Which field you want, counting from zero.</param>
 */
void* Structure::pointerToMember(const void *struct_start, size_t irecord, size_t ifield) {
	return (void*)((char*) struct_start + (getSizeOf() * irecord) + getOffsetOfField(ifield));
}

/** <summary>Copies memory to a field in a structure.</summary>
 * <remarks>This function copies memory from a block pointed to by <c>Src</c> to a 
 * field in an structure.</remarks>
 * <param name="struct_start">Pointer to the start of a structure.</param>
 * <param name="ifield">Which field you want, counting from zero.</param>
 * <param name="Src">Pointer to the source memory location</param>
 */
void Structure::setMember(void *struct_start, size_t ifield, const void *Src) {
	memcpy(pointerToMember(struct_start,ifield),Src,getSizeOfField(ifield));
}

/** <summary>Copies memory to a field in a structure.</summary>
 * <remarks>This function copies memory from a block pointed to by <c>Src</c> to a 
 * field in an structure array.</remarks>
 * <param name="struct_start">Pointer to the start of a structure.</param>
 * <param name="irecord">Which structure you want in the array, counting from zero.</param>
 * <param name="ifield">Which field you want, counting from zero.</param>
 * <param name="Src">Pointer to the source memory location</param>
 */
void Structure::setMember(void *struct_start, size_t irecord, size_t ifield, const void *Src) {
	void * record = NULL;
	char * struct_start_c = (char*) struct_start;
	record = (void*) (struct_start_c + (this->size_of * irecord));
	memcpy(pointerToMember(record,ifield),Src,getSizeOfField(ifield));
}

/** <summary>Converts an array of structures to a string.</summary>
 * <remarks>This function calls the Field::toString() methods for each field in the array of 
 * structures, and splices the resulting strings together by a delimiter. Each structure is then 
 * joined by a record_delimiter. Note that there is no way to specify a format string right now,
 * so the fields are converted to strings using a default format.</remarks>
 * <param name="structptr">Pointer to the start of the array of structs</param>
 * <param name="nrecords">Number of stuctures</param>
 * <param name="field_delim">A string to join fields (typically a comma)</param>
 * <param name="record_delim">A string to join each record</param>
 */
std::string Structure::structsToString(void* structptr, size_t nrecords, std::string field_delim, std::string record_delim) {
	string retstr;
	size_t i,j;
	for(i=0; i<nrecords; i++) {
		for(j=0; j<this->nfields; j++) {
			if(j>0) {
				retstr = retstr + field_delim + this->fields[j]->toString(this->pointerToMember(structptr,i,j));
			} else {
				if(i==0) {
					retstr = this->fields[j]->toString(this->pointerToMember(structptr,i,j));
				} else {
					retstr = retstr + this->fields[j]->toString(this->pointerToMember(structptr,i,j));
				}
			}
		}
		if(i<nrecords-1 && nrecords > 1) {
			retstr = retstr + record_delim;
		}
	}
	return retstr;
}

/** <summary>
 * Finds a field by the field name.
 * </summary>
 * <remarks>
 * Field names are case-sensative. Throws an error if the
 * field does not exist.</remarks>
 * <param name="field_name">Name of the field to search for.</param>
 */
size_t Structure::getFieldIndexByName(std::string field_name) {
	for(size_t i = 0; i<this->nfields; i++) {
		if(this->fields.at(i)->getName() == field_name) {
			return i;
		}
	}

	throw(StructureException("field with name '" + field_name + "' does not exist."));

}

} // namespace tsdb