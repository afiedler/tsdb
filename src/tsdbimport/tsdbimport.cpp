/** \file 
 * <summary>Reads in a CSV or similar delimited file, and appends the records onto an existing series in
 * a TSDB file.</summary>
 * <remarks><p>TSDBimport uses an XML file to define a parser for the delimited file. The parser consists of 
 * a set of TokenFilters and FieldParsers. TokenFilters operate on the tokens from the CSV file to exclude
 * rows. FieldParsers take one or more CSV tokens and parse them into a binary representation that is saved
 * in the HDF5 table.</p>
 * <p>TokenFilters are applied first, in the order they appear in the XML file. If any filter evaluates to
 * true, then that row is skipped and no parsing is attempted. This speeds up the parser, since parsing effort
 * is not spent on rows that would be excluded anyway.</p>
 * <p>FieldParsers act second. FieldParsers take one or more tokens from the CSV file and parse to a binary
 * representation. They correspond to one field in the HDF5 table underlying the timeseries.</p>
 * <p>The following is an example of an XML parser definition, a CSV file excerpt, and tsdbimport command.</p>
 * \code
 * # usdjpy.xml
 *
 * <?xml version="1.0" encoding="UTF-8" ?>
 * <dataimport>
 * <delimparser field_delim=",">
 *     <tokenfilter tokens="2" comparison="NE" value="USD/JPY" />
 *     <fieldparser name="_TDSB_timestamp" type="timestamp" tokens="0,1" format_string="%Y/%m/%d %H:%M:%S%F" />
 *     <fieldparser name="price" type="double" tokens="3" />
 *     <fieldparser name="amount" type="int32" tokens="4" />
 *     <fieldparser name="side" type="int8" tokens="5" />
 * </delimparser>
 * </dataimport>
 * \endcode
 *
 * \code
 * # testdata.csv
 *
 * 2010/01/01,01:01:01.100,USD/JPY,87.56,5,0
 * 2010/01/01,01:01:01.100,USD/JPY,87.58,6,1
 * 2010/01/01,01:01:01.100,EUR/USD,1.56,1,0
 * 2010/01/01,01:01:01.100,EUR/USD,1.58,2,1
 * 2010/01/01,01:01:01.250,USD/JPY,87.59,25,0
 * 2010/01/01,01:01:01.250,USD/JPY,87.61,4,1
 * 2010/01/01,01:01:01.350,EUR/USD,1.54,1,0
 * 2010/01/01,01:01:01.350,EUR/USD,1.55,63,1
 * \endcode
 *
 * \code
 * # Import command
 *
 * > tsdbimport usdjpy.xml testdata.csv usdjpy.tsdb series1
 * \endcode
 *
 * <p>A few things to note: first, in the first field parser line in the XML file, two tokens are joined 
 * with a space before parsing. You can do this when dates and times are separate tokens in your CSV file.
 * Second, tokens in the CSV file are counted starting with 0 for the first token. Last, if a tokenfilter
 * line evaluates to TRUE, that line is discarded. Therefore, in this example, only the USD/JPY lines are
 * kept.</p>
 * 
 * </remarks>
 */

#include <string>
#include <stdexcept>
#include <cmath>
#include <time.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdlib.h>

#include "boost/tokenizer.hpp"

#include "ticpp.h"
#include "tsdb.h"
#include "hdf5.h"

#include "field.h"
#include "fieldparser.h"
#include "structure.h"
#include "recordparser.h"
#include "timeseries.h"

#ifdef WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

#define BYTES_PER_MB 1048576


tsdb::RecordParser* record_parser_from_xml(const std::string parse_instruction_filename, tsdb::Timeseries* out_ts);
void progress_func(double progress, double total, double readspeed, double writespeed);

int main(int argc, char* argv[])
{
	using namespace std;
	using namespace tsdb;
	using namespace ticpp;

	herr_t status;

	/* Parse the command line arguments */
	string in_file, out_file, parse_instruction_filename,tsdb_series;

	if(argc != 5) {
		cerr << "Usage: tsdbimport <parse instructions> <in file> <out file> <out series>" << endl;
		return -1;
	} else {
		parse_instruction_filename = string(argv[1]);
		in_file = string(argv[2]);
		out_file = string(argv[3]);
		tsdb_series = string(argv[4]);
	}

	/* Open the TSDB file */
	hid_t ofh;

	/* Copy the default property list, and make the cache size much larger */
	hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
	int mdc_nelmts;
	size_t rdcc_nbytes,  rdcc_nelmts;
	double rdcc_w0;
	status = H5Pget_cache (fapl, &mdc_nelmts, &rdcc_nelmts, &rdcc_nbytes, &rdcc_w0);
	rdcc_nelmts = 425;
	rdcc_nbytes = 52428800; // 50 MB
	status = H5Pset_cache (fapl, mdc_nelmts, rdcc_nelmts, rdcc_nbytes, rdcc_w0);

	/* Open the file */
	ofh = H5Fopen(out_file.c_str(), H5F_ACC_RDWR, fapl);
	if(ofh < 0) {
		cerr << "Error opening TSDB file: '" << out_file << "'." << endl;
		return -1;
	}

	/* Open the timeseries */
	Timeseries* out_ts = NULL;
	try {
		out_ts = new Timeseries(ofh,tsdb_series);
	} catch (runtime_error &e) {
		cerr << "Unable to open timeseries '" << tsdb_series << "'.\nError:" << e.what() << endl;
		status = H5Fclose(ofh);
		if(status <0) {
			cerr << "Warning: unable to close file. The HDF5 file may be corrupt." << endl;
		}
		H5close();
		return -1;
	}

	/* Begin parsing the file */
	try {
		/* Build the parse instructions from the xml file */
		RecordParser* recordparser = record_parser_from_xml(parse_instruction_filename, out_ts);

		/* Open the input file */
		int ifh = 0;

		#ifdef WIN32
			/* Windows requires a separate call for opening files larger than 2GB */
			ifh = _open(in_file.c_str(),_O_RDONLY | _O_BINARY);
		#else
			ifh = open(in_file.c_str(), O_RDONLY);
		#endif

		if(ifh==-1) {
			cerr << "Unable to open input file at '" << in_file << "'." << endl;
			H5Fclose(ofh);
			H5close();
			return -1;
		}

		// Determine the size of the input file
		long long size = 0;
		#ifdef WIN32
			size = _lseeki64(ifh, 0, SEEK_END);
		#else
			size = lseek(ifh, 0, SEEK_END);
		#endif

		if(size == -1L) {
			cerr << "Unable to seek to end of file '" << in_file << "'." << endl;
			H5Fclose(ofh);
			H5close();
			return -1;
		}

		#ifdef WIN32
			if(_lseeki64(ifh, 0, SEEK_SET) == -1L) {
		#else
			if(lseek(ifh,0,SEEK_SET) == -1L) {
		#endif

			cerr << "Unable to seek to beginning of file '" << in_file << "'." << endl;
			H5Fclose(ofh);
			H5close();
			return -1;
		}
		#ifdef WIN32
			printf("Input file size is %I64d MB\n", size / BYTES_PER_MB);
		#else
			printf("Input file size is %lld MB\n", size / BYTES_PER_MB);
		#endif
		printf("Begin reading file...\n");

		/* Setup the input buffer */
		char *buffer = NULL;
		int buffer_size = 5*BYTES_PER_MB; // 5 MB Buffer
		buffer = (char*) malloc(buffer_size);
		if(buffer == NULL) {
			cerr << "Out of memory on buffer allocation." << endl;
			H5Fclose(ofh);
			H5close();
			return -1;
		}

		int buffer_offset = 0;


		/* Start processing file */
		int ndiscrec = 0; // number of discarded records
		long long linenumber = 0; // Line number
		long long outnumber = 0; // number of records written
		long long completed = 0; // bytes read up to now
		int bytes_read = 0; // bytes read into the buffer
		int nbuflines = 0; // number of lines in the buffer
		int nbufrecords = 0; // number of records in the buffer
		void* records = NULL; // pointer to a block of records
		void* record = NULL; // pointer to a record
		bool parsesuccess = false;
		char* line_start = NULL; // pointer to the start of a line
		bool line_started = false; // true if the start of a line has been encountered
		int i = 0; // a counter
		clock_t starttime,endtime;	

	
		starttime = clock();

		/* Get file */
		std::string line = "";
		while(1) {
			#ifdef WIN32
				bytes_read = _read(ifh, buffer + buffer_offset, buffer_size-buffer_offset);
			#else
				bytes_read = read(ifh, buffer + buffer_offset, buffer_size-buffer_offset);
			#endif

			if(bytes_read < 0) {
				cerr << "Error reading from input file '" << in_file << "'. Already read " <<
					completed << " bytes. Must close!" << endl;
				H5Fclose(ofh);
				H5close();
				return -1;
			}

			if(bytes_read == 0) {
				// end of file reached!
				break;
			}

			completed = completed + bytes_read;

			progress_func((double) completed, (double) size, 
				((double) completed/BYTES_PER_MB)/((double) ((clock()-starttime)/CLOCKS_PER_SEC)),
				((double) outnumber)/((double) ((clock()-starttime)/CLOCKS_PER_SEC)));
			
			// Count number of lines in the buffer
			nbuflines = 0;
			for(i=0; i < bytes_read; i++) {
				if (buffer[i] == '\n') {
					nbuflines++;
				}
			}

			// Allocate some space for the the records. Have to assume that each line is going
			// to be a record
			size_t recordsize = out_ts->structure()->getSizeOf();
			records = malloc( recordsize * ((size_t)(nbuflines+1)) );
			if(records == NULL) {
				cerr << "Out of memory in records allocation." << endl;
				cerr << "Already read " << completed << " bytes of file '" << in_file << "'.";
				H5Fclose(ofh);
				H5close();
				return -1;
			}

			// Loop through the buffer and parse the lines 
			nbufrecords = 0;
			line_start = buffer;
			line_started = true;
			for(i=0;i < (bytes_read + buffer_offset); i++) {
				// If a line has been started, find the end of the line and null-terminate it, and parse it
				if (line_started) {
					if (buffer[i] == '\r' || buffer[i] == '\n') {
						buffer[i] = '\0';
						line = line_start; // convert a C string into a std::string
						linenumber++;

						/* Try to parse the line, unless it is a blank line */
						if(line.size() != 0 ) {
							try {
								parsesuccess = recordparser->parseString(line,
									(((char*)records) + (nbufrecords*recordsize)));
							} catch (std::runtime_error &e) {
								/* Not able to parse? Output the error, and skip that line */
								cerr << "Error parsing line. Line was #" << linenumber << ":\n'" << line << "'\n" << "Error was:\n" << e.what() << endl;
								parsesuccess = false;
							} catch(std::exception &e) {
								/* Not able to parse? Output the error, and skip that line */
								cerr << "Error parsing line. Line was #" << linenumber << ":\n'" << line << "'\n" << "Error was:\n" << e.what() << endl;
								parsesuccess = false;
							}
						} else {
							parsesuccess = false;
						}

						if(parsesuccess) {
							// record is good. Increment records counter
							nbufrecords++;
							parsesuccess = false;
						}

						line_started = false;
					}
				} else {
					// A line has just ended, so look for one non-newline or null char to start a new line
					if(buffer[i] != '\n' && buffer[i] != '\r' && buffer[i] != '\0') {
						line_started = true;
						line_start = buffer + i;
					}
				}
			}

			if (line_started) {
				// A line has been started, but it has not terminated in this buffer. Move the remaining
				// "scraps" to the top of the buffer and read in more of the file.
				memcpy(buffer,line_start,buffer_size -(line_start-buffer));

				// set the buffer offset to the length of the partial line that was just
				// moved to the top
				buffer_offset = buffer_size -(line_start-buffer);
			} else {
				buffer_offset = 0;
			}

			// Append the records to the table
			ndiscrec = out_ts->appendRecords(nbufrecords,records,true);

			if (ndiscrec > 0) {
				/* Some records were discarded because they overlapped. Warn the user */
				cerr << ndiscrec << " record(s) discarded because they were misordered." << endl;
				// This prints the records: cerr << out_ts->structure()->structsToString(records,ndiscrec,",","\n") << endl;
			}
			outnumber = outnumber + nbufrecords - ndiscrec;

			// Free up the records buffer
			free(records);
			records = NULL;

		}
		endtime = clock();
		free(buffer);
	} catch(std::runtime_error &e) {
		cerr << "Caught runtime error:" << endl;
		cerr << e.what();
		status = H5Fclose(ofh);
		if(status < 0) {
			cerr << "Warning: error closing TSDB file. There may be data corruption." << endl;
		}
		return -1;
	} catch(std::exception &e) {
		cerr << "Caught exception:" << endl;
		cerr << e.what();
		status = H5Fclose(ofh);
		if(status < 0) {
			cerr << "Warning: error closing TSDB file. There may be data corruption." << endl;
		}
		return -1;
	} catch(...) {
		cerr << "Unknown error" << endl;
		status = H5Fclose(ofh);
		if(status < 0) {
			cerr << "Warning: error closing TSDB file. There may be data corruption." << endl;
		}
		return -1;
	}

	status = H5Fclose(ofh);
	if(status < 0) {
		cerr << "Warning: error closing TSDB file. There may be data corruption." << endl;
		return -1;
	}

	return 0;


	
}

tsdb::RecordParser* record_parser_from_xml(const std::string parse_instruction_filename, tsdb::Timeseries* out_ts) {
	using namespace std;
	using namespace ticpp;

	Document doc = Document(parse_instruction_filename);
	doc.LoadFile();

	cout << "Loaded '" << parse_instruction_filename << "'." << endl;
	cout << "Creating parser..." << endl;

	/* Create the RecordParser */
	tsdb::RecordParser* recordparser = new tsdb::RecordParser();
	recordparser->setRecordStructure(out_ts->structure().get());

	Iterator<Element> child;
	string name;
	string value;
	auto_ptr<Node> delimparser;
	for(child = child.begin(doc.FirstChildElement()); child != child.end(); child++) {
		child->GetValue(&value);
		if(value == "delimparser") {
			delimparser = child->Clone();
			break;
		}
	}

	string delim = delimparser->ToElement()->GetAttribute("field_delim");
	if(delim == "") {
		delim = ",";
	}

	string escape = delimparser->ToElement()->GetAttribute("escape_chars");
	if(escape == "") {
		escape="\\";
	}

	string quote = delimparser->ToElement()->GetAttribute("quote_chars");
	if(quote == "") {
		quote="\"'";
	}
	
	string mode = delimparser->ToElement()->GetAttribute("parse_mode");
	if(mode == "extended") {
		recordparser->setSimpleParse(false);
		recordparser->setDelimiter(delim);				
		recordparser->setEscapeCharacter(escape);
		recordparser->setQuoteCharacter(quote);
		cout << "   - field delimiter(s): '" << delim << "'" << endl;
		cout << "   - quote character(s): '" << quote << "'" << endl;
		cout << "   - escape character(s): '" << escape << "'" << endl;
	} else {
		recordparser->setSimpleParse(true);
		recordparser->setDelimiter(delim.substr(0,1));
		cout << "   - field delimiter: '" << delim.substr(0,1) << "'" << endl;
	}
	



	

	cout << "   Processing parser elements:" << endl;


	/* Loop through the RecordParser and look for TokenFilters or FieldParsers */
	typedef boost::tokenizer< boost::escaped_list_separator<char> > Tokenizer;
	string tokens,comparison,format_string,type,missing_token_replacement;
	bool missing_tokens_ok = false;

	vector<string> vec;
	vector<size_t> apply_to_tokens;
	size_t i;
	for(child = child.begin(delimparser.get()); child != child.end(); child++) {
		child->GetValue(&value);


		if(value == "tokenfilter") {
			
			
			tokens = child->GetAttribute("tokens");
			comparison = child->GetAttribute("comparison");
			value = child->GetAttribute("value");
			
			Tokenizer tok(tokens);
			vec.assign(tok.begin(),tok.end());

			apply_to_tokens.clear();
			for(i=0;i<vec.size();i++) {
				apply_to_tokens.push_back(atoi(vec.at(i).c_str()));
			}

			/* Write out some information */
			cout << "      - TokenFilter:" << endl;
			cout << "         apply to tokens: (";
			for(i=0;i<apply_to_tokens.size();) {
				cout << apply_to_tokens.at(i);
				i++;
				if(i<apply_to_tokens.size()) {
					cout << ",";
				}
			}
			cout << ")" << endl;

			if(comparison == "NE") {
				recordparser->addTokenFilter(new tsdb::TokenFilter(apply_to_tokens,
					tsdb::TokenFilter::NOT_EQUAL_TO, value));
				cout << "         comparison: NOT_EQUAL_TO" << endl;

			} else if(comparison == "EQ") {
				recordparser->addTokenFilter(new tsdb::TokenFilter(apply_to_tokens,
					tsdb::TokenFilter::EQUAL_TO, value));
				cout << "         comparison: EQUAL_TO" << endl;

			} else {
				cout << "         comparison not recognized!" << endl;
				throw(runtime_error("comparison operator in TokenFilter not recognized"));
			}

			cout << "         value: '" << value << "'" << endl;

		} else if(value == "fieldparser") {

			tokens = child->GetAttribute("tokens");
			missing_tokens_ok=false;

			Tokenizer tok(tokens);
			vec.assign(tok.begin(),tok.end());
			apply_to_tokens.clear();
			for(i=0;i<vec.size();i++) {
				apply_to_tokens.push_back(atoi(vec.at(i).c_str()));
			}

			/* Write out some information */
			cout << "      - FieldParser:" << endl;
			cout << "         apply to tokens: (";
			for(i=0;i<apply_to_tokens.size();) {
				cout << apply_to_tokens.at(i);
				i++;
				if(i<apply_to_tokens.size()) {
					cout << ",";
				}
			}
			cout << ")" << endl;

			name = child->GetAttribute("name");
			format_string = child->GetAttribute("format_string");
			type = child->GetAttribute("type");
			if(child->HasAttribute("missing_token_replacement")) {
				missing_token_replacement = child->GetAttribute("missing_token_replacement");
				missing_tokens_ok = true;
			}

			if(type == "timestamp") {
				recordparser->addFieldParser(new tsdb::TimestampFieldParser(apply_to_tokens, format_string,
					name));
				cout << "         type: Timestamp" << endl;
				cout << "         format string: '" << format_string << "'" << endl;
			} else if(type == "string") {
				recordparser->addFieldParser(new tsdb::StringFieldParser(apply_to_tokens, name));
				cout << "         type: String" << endl;
			} else if(type == "int32") {
				recordparser->addFieldParser(new tsdb::Int32FieldParser(apply_to_tokens.at(0), name));
				cout << "         type: Int32" << endl;
			} else if(type == "int8") {
				recordparser->addFieldParser(new tsdb::Int8FieldParser(apply_to_tokens.at(0), name));
				cout << "         type: Int8" << endl;
			} else if(type == "double") {
				recordparser->addFieldParser(new tsdb::DoubleFieldParser(apply_to_tokens.at(0), name));
				cout << "         type: Double" << endl;
			} else if(type == "char") {
				recordparser->addFieldParser(new tsdb::CharFieldParser(apply_to_tokens.at(0), name));
				cout << "         type: Char" << endl;
			} else {
				cout << "         type: not recognized!" << endl;
				throw(runtime_error("type in FieldParser not recognized"));
			}
			
			if(missing_tokens_ok) {
				recordparser->fieldParsers().back()->setMissingTokenReplacement(missing_token_replacement);
				cout << "         missing_token_replacement: '" << missing_token_replacement << "'" << endl;
			}

			cout << "         name: '" << name << "'" << endl;
		}
	}
	
	return recordparser;
	cout << "   finished processing parser elements." << endl;
}


void progress_func(double progress, double total, double readspeed, double writespeed) {
	int totaldots = 20;
	double fraction_complete = progress/total;

	int dots = static_cast<int>(ceil(fraction_complete * totaldots));

	// create the meter
	int ii = 0;
	printf("%3.0f%% [", fraction_complete*100);

	for( ; ii < dots; ii++) {
		printf("=");
	}

	for( ; ii <totaldots; ii++) {
		printf(" ");
	}

	printf("] read: %3.1f MB/s, write: %3.1f Krec/s   \r", readspeed, writespeed/1000);
	fflush(stdout);
}