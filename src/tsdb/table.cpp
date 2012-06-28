/* STL includes */
#include <sstream>
#include <string>
#include <vector>
#include <boost/make_shared.hpp>

/* HDF5 Includes */
#include "hdf5.h"
#include "hdf5_hl.h"

/* TSDB includes */
#include "table.h"
#include "bufferedrecordset.h"



using namespace std;


namespace tsdb {

/* ====================================================================
 * class Table - wraps a HDF5 High Level Table
 * ====================================================================
 */

/** <summary>Creates a new table at from a Structure</summary>
 * <remarks>Creates a new table at <c>new_loc_id</c> based on a Structure. If there are errors
 * creating the table, this constructor will throw a TableException.</remarks>
 * <param name="new_loc_id">A HDF5 <c>hid_t</c> specifying the location of the table (either a group id or file id)</param>
 * <param name="new_title">Title of the table</param>
 * <param name="new_name">Name of the table. This must conform to HDF5 naming rules.</param>
 * <param name="new_record_struct">Structure of the Table</param>
 */
Table::Table(hid_t _loc_id, std::string _name, std::string _title, boost::shared_ptr<tsdb::Structure> _structure) {
	this->my_loc_id = _loc_id;
	this->my_name = _name;
	this->my_title = _title;
	this->my_structure = _structure;

	herr_t status;
	status = H5TBmake_table(my_title.c_str(),my_loc_id,my_name.c_str(),
		my_structure->getNFields(),0,my_structure->getSizeOf(),
		const_cast<const char**>(my_structure->getNameOfFieldsAsArray()),
		my_structure->getOffsetOfFieldsAsArray(),my_structure->getTypeOfFieldsAsArray(),4096,NULL,1,NULL);

	/* Check for errors in the make_table command */
	if(status < 0) {
		throw( TableException("Error in H5TBmake_table.") );
	}

	/* Add some attributes to the table describing the field types */
	stringstream field_type_key;
	string field_type_value;
	for(size_t i = 0; i<my_structure->getNFields(); i++) {
		field_type_key.str("");
		field_type_key << "FIELD_" << i << "_TYPE";
		field_type_value = my_structure->getField(i)->getTSDBType();
		status = H5LTset_attribute_string(my_loc_id,my_name.c_str(),field_type_key.str().c_str(),field_type_value.c_str());
		if(status < 0) {
			throw( TableException("Error in H5LTset_attribute_string.") );
		}
	}
	
	// Append buffer is empty until someone tries to append a record
	my_nappendbuf = 0;
	
}

/** <summary>Opens an already existing table</summary>
 * <remarks>Opens a new table from <c>tbl_loc_id</c>. If there are errors
 * opening the table or if the table does not exist, this constructor will throw a TableException.</remarks>
 * <param name="tbl_loc_id">A HDF5 <c>hid_t</c> specifying the location of the table (either a group id or file id)</param>
 * <param name="tbl_name">Name of the table to open</param>
 */
Table::Table(hid_t _loc_id, std::string _name) {
	herr_t status;
	hsize_t nfields, nrecords;
	hsize_t i = 0;
	
	if(!Table::exists(_loc_id, _name)) {
		throw( TableException("Table does not exist.") );
	}

	/* Get information about the table and it's fields */
	status = H5TBget_table_info(_loc_id, _name.c_str(), &nfields, &nrecords);
	if(status < 0) {
		throw( TableException("Error in H5TBget_table_info.") );
	}

	size_t * field_sizes = new size_t[(size_t) nfields];
	size_t * field_offsets = new size_t[(size_t) nfields];
	size_t type_size;
	vector<Field*> fields;

	status = H5TBget_field_info(_loc_id, _name.c_str(), NULL, field_sizes, field_offsets, &type_size);
	if(status < 0) {
		throw( TableException("Error in H5TBget_field_info.") );
	}

	/* Get the table title */
	char* tbl_title;
	hsize_t dims;
	H5T_class_t type_class;
	size_t attr_size;

	status = H5LTget_attribute_info(_loc_id,_name.c_str(),"TITLE",&dims,&type_class,&attr_size);
	if(status < 0) {
		throw( TableException("Error in H5LTget_attribute_info. Table might be missing a TITLE attribute") );
	}

	tbl_title = new char[attr_size];

	status = H5LTget_attribute_string(_loc_id,_name.c_str(),"TITLE",tbl_title);
	if(status < 0) {
		throw( TableException("Error in H5LTget_attribute_string. Table might be missing a TITLE attribute") );
	}

	/* Load the table's fields and make its Structure */
	stringstream field_type_key;
	stringstream field_name_key;
	vector<size_t> offsets;

	char* field_type_value;
	char* field_name_value;
	bool fieldset = false;
	for(i=0;i<nfields;i++) {
		/* Get the field type */
		field_type_key.str("");
		field_type_key << "FIELD_" << i << "_TYPE";

		status = H5LTget_attribute_info(_loc_id,_name.c_str(),field_type_key.str().c_str(),&dims,&type_class,&attr_size);
		if(status < 0) {
			throw( TableException("Error in H5LTget_attribute_info while getting field types.") );
		}

		field_type_value = new char[attr_size];

		status = H5LTget_attribute_string(_loc_id,_name.c_str(),field_type_key.str().c_str(),field_type_value);
		if(status < 0) {
			throw( TableException("Error in H5LTget_attribute_string while getting field types.") );
		}

		/* Get the field name */
		field_name_key.str("");
		field_name_key << "FIELD_" << i << "_NAME";

		status = H5LTget_attribute_info(_loc_id,_name.c_str(),field_name_key.str().c_str(),&dims,&type_class,&attr_size);
		if(status < 0) {
			throw( TableException("Error in H5LTget_attribute_info while getting field names.") );
		}

		field_name_value = new char[attr_size];
		status = H5LTget_attribute_string(_loc_id,_name.c_str(),field_name_key.str().c_str(),field_name_value);
		if(status < 0) {
			throw( TableException("Error in H5LTget_attribute_string while getting field names.") );
		}

		if(strcmp(field_type_value,"Timestamp") == 0) {
			fields.push_back(new TimestampField((string) field_name_value));
			fieldset = true;
		}

		if(strcmp(field_type_value,"Record") == 0) {
			fields.push_back(new RecordField((string) field_name_value));
			fieldset = true;
		}

		if(strcmp(field_type_value,"Int32") == 0) {
			fields.push_back(new Int32Field((string) field_name_value));
			fieldset = true;
		}

		if(strcmp(field_type_value,"Int8") == 0) {
			fields.push_back(new Int8Field((string) field_name_value));
			fieldset = true;
		}

		if(strcmp(field_type_value,"Char") == 0) {
			fields.push_back(new CharField((string) field_name_value));
			fieldset = true;
		}

		if(strcmp(field_type_value,"Double") == 0) {
			fields.push_back(new DoubleField((string) field_name_value));
			fieldset = true;
		}

		if(strcmp(field_type_value,"Date") == 0) {
			fields.push_back(new DateField((string) field_name_value));
			fieldset = true;
		}

		if(strstr(field_type_value,"String(") == field_type_value) {
			// need to determine the size of the string
			char * paren = strstr(field_type_value,")");
			*paren = '\0';
			paren = field_type_value + 7;
			int field_size = 0;
			field_size = atoi(paren);

			if(field_size < 1) {
				throw( TableException("String field size is invalid.") );
			}

			fields.push_back(new StringField((string) field_name_value, field_size));
			fieldset = true;
		}

		if(fieldset == false) {
			throw( TableException("A field had an unsupported field type.") );
		}

		offsets.push_back(field_offsets[i]);
		delete field_name_value, field_type_value;
	}

	// Set up the table object
	my_loc_id = _loc_id;
	my_name = _name;
	my_structure = boost::shared_ptr<tsdb::Structure>(new Structure(fields,offsets,type_size));
	my_title = std::string(tbl_title);
	

	// Clean up some memory
	delete  tbl_title, field_sizes, field_offsets;

	// NOTE: we are not cleaning up the individual fields. We pass ownership of them to my_structure.

	// Append buffer is empty until we attempt to append a record
	my_nappendbuf = 0;
}


/** <summary>Checks if a Table exists at <c>loc_id</c></summary>
 * <remarks>Checks if a Table exists. Returns <c>true</c> if it does, false otherwise.</remarks>
 * <param name="loc_id">A HDF5 <c>hid_t</c> specifying the location of the table (either a group id or file id)</param>
 * <param name="name">Name of the table</param>
 */
bool Table::exists(hid_t loc_id, std::string name) {
	herr_t status = 0;
	hsize_t nfields;
	hsize_t nrecords;
	
	/* Error printing off */
	status = H5Eset_auto2(H5E_DEFAULT,NULL,NULL);
	if(status <0) {
		throw( TableException("There was a problem redirecting error printing."));
	}

	status = H5TBget_table_info(loc_id, name.c_str(),&nfields,&nrecords);

	/* Error printing on */
	status = H5Eset_auto2(H5E_DEFAULT,(H5E_auto2_t) H5Eprint, stderr);
	if(status < 0) {
		throw( TableException("There was a problem redirecting error printing."));
	}

	if(status >= 0) {
		return true;
	} else {
		return false;
	}
}

/** <summary>Appends records to a table</summary>
 * <remarks>Appends records to the end of a table.</remarks>
 * <param name="nrecords">The number of records to append.</param>
 * <param name="records">Pointer to a block of memory containing the records</param>
 */
void Table::appendRecords(size_t nrecords, void* records) {
	herr_t status = H5TBappend_records(my_loc_id,my_name.c_str(),nrecords,my_structure->getSizeOf(),
		my_structure->getOffsetOfFieldsAsArray(),my_structure->getSizeOfFieldsAsArray(),records);
	if(status < 0) {
		throw( TableException("Error in H5TBappend_records.") );
	}
}

/** <summary>Gets the number of records in the table</summary> */
hsize_t Table::size(void) {
	hsize_t tbl_nfields,tbl_nrecords;
	herr_t status;
	status = H5TBget_table_info(my_loc_id,my_name.c_str(),&tbl_nfields,&tbl_nrecords);
	if(status < 0) {
		throw( TableException("Error in H5TBget_table_info.") );
	}
	return tbl_nrecords;
}

/** <summary>Gets a set of records from the Table</summary>
* <remarks><p>This function takes the address of a pointer to a set or records. It will set the memory
* at that address to an address of a newly allocated block of memory containing the requested records.</p>
* <p>If there is a problem reading the records, this method will throw a TableException and memory will not
* be allocated. If no exception is returned, the records have been fetched and memory allocated. You
* must then remember to free this memory.</p></remarks>
* <param name="first">First record index</param>
* <param name="last">Last record index</param>
* <param name="records">Pointer to a pointer to an array of records</param>
*/
void Table::getRecords(hsize_t first, hsize_t last, void** records) {
	hsize_t tbl_nrecords;
	herr_t status;
	tbl_nrecords = size();

	if(first >= tbl_nrecords || last >= tbl_nrecords) {
		throw( TableException("Records requested outside the bounds of the table.") );
	}

	if(last < first) {
		throw( TableException("The last record requested is before the first record requested.") );
	}
	
	*records = malloc(my_structure->getSizeOf() * ( (size_t) (last-first + 1) ));
	if(*records == NULL) {
		throw(TableException("not enough memory in Table::getRecords"));
	}

	status = H5TBread_records(my_loc_id, my_name.c_str(), first, last-first+1, my_structure->getSizeOf(),
		my_structure->getOffsetOfFieldsAsArray(), my_structure->getSizeOfFieldsAsArray(), *records);

	if(status < 0) {
		free(*records);
		throw( TableException("Error in H5TBread_records.") );
	}
}

/** <summary>Gets a set of records from the Table</summary>
* <remarks><p>This function takes the address of a pointer to a set or records. It will set the memory
* at that address to an address of a newly allocated block of memory containing the requested records.</p>
* <p>If there is a problem reading the records, this method will throw a TableException and memory will not
* be allocated. If no exception is returned, the records have been fetched and memory allocated. You
* must then remember to free this memory.</p></remarks>
* <param name="first">First record index</param>
* <param name="last">Last record index</param>
* <param name="records">Pointer to a pointer to an array of records</param>
*/
tsdb::MemoryBlockPtr Table::recordsAsMemoryBlockPtr(hsize_t first, hsize_t last) {
	hsize_t tbl_nrecords;
	herr_t status;
	tbl_nrecords = this->size();

	if(first >= tbl_nrecords || last >= tbl_nrecords) {
		throw( TableException("Records requested outside the bounds of the table.") );
	}

	if(last < first) {
		throw( TableException("The last record requested is before the first record requested.") );
	}
	
	boost::shared_ptr<tsdb::MemoryBlock> recmemblk = 
		boost::make_shared<tsdb::MemoryBlock>(my_structure->getSizeOf() * ( (size_t) (last-first + 1) ));
	
	status = H5TBread_records(my_loc_id, my_name.c_str(), first, last-first+1, my_structure->getSizeOf(),
		my_structure->getOffsetOfFieldsAsArray(), my_structure->getSizeOfFieldsAsArray(), recmemblk->raw());

	if(status < 0) {
		throw( TableException("Error in H5TBread_records.") );
	}

	return tsdb::MemoryBlockPtr(recmemblk,0);
}

/** <summary>Gets a RecordSet from a Table</summary>
 * <remarks>This function throws an exception if there were any problems getting the RecordSet.</remarks>
 * <param name="first">First record index</param>
 * <param name="last">Last record index</param>
 */
tsdb::RecordSet Table::recordSet(hsize_t first, hsize_t last) {
	tsdb::MemoryBlockPtr recblkptr = this->recordsAsMemoryBlockPtr(first,last);
	return tsdb::RecordSet(recblkptr,(size_t)(last-first+1), my_structure);
}



/** <summary>Gets the last record of the Table</summary>
 * <remarks>Returns a pointer to the last record of the Table. This memory has been allocated for you,
 * but you need to remember to <c>free()</c> it when you are finished. If, for any reason, the last record
 * cannot be retreived, NULL is returned and no memory is allocated.</remarks>
 */
void * Table::getLastRecord(void) {
	void * recordPtr = NULL;
	herr_t status;

	hsize_t tbl_nrecords = 0;
	tbl_nrecords = size();
	
	if(tbl_nrecords == 0) {
		return NULL;
	} else {
		recordPtr = malloc(my_structure->getSizeOf());
		if(recordPtr == NULL) {
			throw(TableException("not enough memory"));
		}
	}

	// Get the last record of the table
	status=H5TBread_records(my_loc_id,my_name.c_str(),tbl_nrecords-1,1,my_structure->getSizeOf()
		,my_structure->getOffsetOfFieldsAsArray(),my_structure->getSizeOfFieldsAsArray(),recordPtr);
	if(status < 0) {
		free(recordPtr);
		throw( TableException("Error in H5TBread_records.") );
	}

	return recordPtr;
}

/** <summary>Returns the title of the table</summary> */
std::string Table::title(void) {
	return my_title;
}

/** <summary>Destructor</summary>
 * <remarks>Note that this flushes the append buffer to disk.</remarks>
 */
Table::~Table(void) {
	this->flushAppendBuffer();
}

/** <summary>Returns the Structure of the table</summary> */
boost::shared_ptr<tsdb::Structure> Table::structure() {
	return my_structure;
}

void Table::appendRecord(tsdb::Record &_record) {
	// Validate that the _record uses the same Structure as the Timeseries
	if(_record.structure() != my_structure) {
		throw std::runtime_error("attempted to append record with different structure");
	}

	if(!my_append_buffer.isAllocated()) {
		// allocate some space for the append buffer
		my_append_buffer = tsdb::MemoryBlock(my_structure->getSizeOf() * APPEND_BUFFER_SIZE);
		my_nappendbuf = 0;
	} 

	void * save_spot = NULL;
	save_spot = my_append_buffer.raw() + 
		(my_structure->getSizeOf() * my_nappendbuf);

	// copy the record to the appropriate spot
	memcpy(save_spot,_record.memoryBlockPtr().raw(),_record.structure()->getSizeOf());
	my_nappendbuf++;

	if(my_nappendbuf == APPEND_BUFFER_SIZE) {
		// flush the append buffer
		flushAppendBuffer();
	}

}

void Table::flushAppendBuffer(void) {
	if(my_append_buffer.isAllocated() && my_nappendbuf > 0) {
		appendRecords(my_nappendbuf,my_append_buffer.raw());
		my_nappendbuf = 0;
	}
}


/** <summary>Gets a BufferedRecordSet </summary> */
tsdb::BufferedRecordSet Table::bufferedRecordSet(hsize_t first, hsize_t last) {
	return tsdb::BufferedRecordSet(this,first,last);
}

size_t Table::appendBufferSize() {
	return my_nappendbuf;
}


} // namespace tsdb 