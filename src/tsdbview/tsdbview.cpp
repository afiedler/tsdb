// tsdbview.cpp : Defines the entry point for the console application.
//

#include "boost/date_time/posix_time/posix_time.hpp"
#include "tsdb.h"
#include "timeseries.h"
#include "hdf5.h"



int main(int argc, char* argv[])
{
	using namespace std;
	using namespace boost::posix_time;
	using namespace tsdb;
	hid_t ofh; 
	string filename,series,start_date_s,end_date_s;
	if(argc < 5) {
		cerr << "Error: Not enough arguments." << endl;
		cout << "Usage: tsdbview <filename> <series> <start_date> <end_date>." << endl;
		cout << "Date format is YYYYMMDDThhmmssffff. Fractional seconds optional." << endl <<
			"For example, 20080201T010000" << endl;
		return -1;
	}

	filename = argv[1]; series = argv[2]; start_date_s = argv[3]; end_date_s = argv[4];

	ofh = H5Fopen(filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	if(ofh < 0) {
		cerr << "Error: Unable to opening TSDB file: '" << filename << "'." << endl;
		return -1;
	}
	try {
		// Open the timeseries
		Timeseries* Ts = new Timeseries(ofh,series);

		// Parse the dates
		ptime start(from_iso_string(start_date_s));
		ptime end(from_iso_string(end_date_s));

		hsize_t start_id = 0;
		hsize_t end_id = 0;
		hsize_t nrecords = 0;
		herr_t status;


		if(start > end) {
			throw(TimeseriesException("Start timestamp cannot be greater than end timestamp."));
		}


		status = Ts->recordId_GE(start, &start_id);
		if(status == -1) {
			throw(TimeseriesException("The start timestamp is greater then the last record in the timeseries."));
		}

		status = Ts->recordId_LE(end, &end_id);
		if(status == -1) {
			throw(TimeseriesException("The end timestamp was less than the first record in the timeseries."));
		}

		status = Ts->recordId_GE(tsdb::ptime_to_timestamp(end)+1, &end_id);
		if(status == -1) {
			end_id = Ts->getNRecords() - 1;
		} else {
			end_id = end_id -1;
		}
	
		size_t record_block = 10000; // how many records to load at a time


	
		/* Load one block of records at a time, and print it out to stdout. */
		void * records = NULL;
		unsigned int k = 0;
		hsize_t i,j;
		for(i=start_id;i<=end_id; i+=record_block) {
			if((i+record_block-1) <= end_id) {
				j = i + record_block -1;
			} else {
				j=end_id;
			}

			records = Ts->getRecordsById(i,j);
			nrecords = j-i+1;

			for( k =0; k<nrecords; k+=100) {
				cout << i+k << Ts->structure()->structsToString(Ts->structure()->pointerToMember(records,k,0),
					(((unsigned int) nrecords) - k < 100 ? ((unsigned int) nrecords)-k : 100),",","\n") << endl;
			}
			free(records);
		}
	} catch(std::exception &e) {
		cerr << "Exception:" << endl;
		cerr << e.what() << endl;
	}
	H5Fclose(ofh);
	return 0;
}

