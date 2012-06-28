/** \file 
 * <summary>Creates a new TSDB file and series, or, if the file exists, a new series in the file.</summary>
 * <remarks><code>Usage: tsdbcreate <filename> <series> (<field type> <field name>)</code>
 * <p>List field type and field names in pairs. Valid field types are:</p>
 * <ul>
 * <li>int8&ndash;8-bit signed integer</li>
 * <li>int32&ndash;32-bit signed integer</li>
 * <li>double&ndash;64-bit IEEE floating point number</li>
 * <li>char&ndash;8-bit signed integer representing a ANSI character</li>
 * <li>timestamp&ndash;64-bit signed integer representing milliseconds before or after Jan 1, 1970 00:00:00</li>
 * <li>record&ndash;64-bit signed integer representing a record id (used internally)</li>
 * </ul>
 * <p>An example:</p>
 * <code>&gt; tsdbcreate usdjpy.tsdb series1 double price int32 amount int8 side</code>
 * <p><strong>NOTE:</strong> A timestamp field called &ldquo;_TSDB_timestamp&rdquo; is automatically added
 * to the start of the field list. This field is used to order the records in the timeseries.</p>
 * </remarks>
 * 
 */

#include <string>
#include <vector>
#include <algorithm>
#include <cctype>
#include <sys/stat.h>
#include <boost/make_shared.hpp>

#include "tsdb.h"
#include "hdf5.h"

#include "field.h"
#include "structure.h"
#include "timeseries.h"


int main(int argc, char* argv[])
{
	using namespace std;
	using namespace tsdb;

	if(argc < 5) {
		cerr << "One or more fields required." << endl;
		cerr << "Usage: tsdbcreate <filename> <series> (<field type> <field name>)..." << endl;
		return -1;
	}
	if((argc-3) % 2 != 0) {
		cerr << "Each field must have a type and name." << endl;
		cerr << "Usage: tsdbcreate <filename> <series> [<field type> <field name>]..." << endl;
		return -1;
	}

	/* Get the filename and series names */
	string filename,series;
	filename = string(argv[1]);
	series = string(argv[2]);

	/* Get the fields */
	string field_type;
	string field_name;

	vector<Field*> fields;

	/* Add the timestamp field */
	fields.push_back(new TimestampField("_TSDB_timestamp"));

	int i;
	for(i=1;(2*i + 2)<argc;i++) {
		field_type = string(argv[(2*i+1)]);
		cout << "field type: " << field_type << endl;
		
		field_name = string(argv[(2*i+2)]);
		cout << "field name: " << field_name << endl;

		std::transform(field_type.begin(),field_type.end(),field_type.begin(), ::toupper);
		if(field_type == "TIMESTAMP") {
			fields.push_back(new TimestampField(field_name));
		} else if(field_type == "INT32") {
			fields.push_back(new Int32Field(field_name));
		} else if(field_type == "INT8") {
			fields.push_back(new Int8Field(field_name));
		} else if(field_type == "DOUBLE") {
			fields.push_back(new DoubleField(field_name));
		} else if(field_type == "CHAR") {
			fields.push_back(new CharField(field_name));
		} else if(field_type == "RECORD") {
			fields.push_back(new RecordField(field_name));
		} else if(field_type.find_first_of("STRING(") == 0) {
			// The user wants a string type. Determine the size
			std::string size_str = field_type.substr(7,field_type.length()-8);
			int size = atoi(size_str.c_str());
			if(size < 1) {
				cerr << "Size of " << size << " is too small." << endl;
				return -1;
			}

			fields.push_back(new StringField(field_name,size));
		} else {
			cerr << "Incorrect field type." << endl;
			return -1;
		}
	}

	/* Does the file exist? */

	struct stat finfo;
	int intstat;
	intstat = stat(filename.c_str(),&finfo);
	hid_t ofh;
	if(intstat != 0) {
		// Try to create the file
		ofh = H5Fcreate(filename.c_str(),H5F_ACC_EXCL,H5P_DEFAULT,H5P_DEFAULT);
		if(ofh < 0) {
			cerr << "Error creating TSDB file: '" << filename << "'." << endl;
			return -1;
		}
	} else {
		// Try to open the file
		ofh = H5Fopen(filename.c_str(), H5F_ACC_RDWR, H5P_DEFAULT);
		if(ofh < 0) {
			cerr << "Error opening TSDB file: '" << filename << "'." << endl;
			return -1;
		}
	}

	/* Create timeseries */
	herr_t status;
	try {
		boost::shared_ptr<Structure> st = boost::make_shared<Structure>(fields,false); /* Note: no memory alignment for better space utilization */
		Timeseries ts = Timeseries(ofh,series,"",st);
	} catch( TableException &ex ) {
		cerr << ex.what();
		status = H5Fclose(ofh);
		if(status < 0){
			cerr << "Warning: could not close file." << endl;
		}
		return -1;
	}
	

	status = H5Fclose(ofh);
	if(status < 0) {
		cerr << "Warning: could not close file." << endl;
	}


	return 0;

}