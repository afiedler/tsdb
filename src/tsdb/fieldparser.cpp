/* STL Classes */
#include <iostream>
#include <locale>
#include <stdlib.h>

/* Boost */
#include "boost/date_time.hpp"

/* TSDB Includes */
#include "tsdb.h"
#include "recordparser.h"
#include "fieldparser.h"


namespace tsdb {
/* ====================================================================
 * class FieldParser - FieldParser base class.
 * ====================================================================
 */

/** <summary>
 * Unused. Create a subclass of FieldParser instead.
 * </summary>
 */
FieldParser::FieldParser(void)
{
}

FieldParser::~FieldParser(void)
{
}

void FieldParser::writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record) {
}

void FieldParser::setMissingTokenReplacement(std::string _missing_token_replacement) {
	this->missing_tokens_ok = true;
	this->missing_token_replacement = _missing_token_replacement;
}


/** <summary>
 * Binds the FieldParser to a RecordParser. 
 * </summary>
 * <remarks><p>Looks up the field name in the record Structure
 * and saves the index so subsequent uses of the FieldParser don't need to make an expensive
 * field name lookup. If the field name is not found, the record Structure will throw an 
 * exception, which will be rethrown by this method.</p>
 * <p>TODO: Make this a virtual function overloaded by the subclasses so that Field type can 
 * be checked before binding</p>
 * </remarks>
 * <param name="new_record_parser">Pointer to a RecordParser to bind to</param>
 */
void FieldParser::bindToRecordParser(tsdb::RecordParser* new_record_parser) {
	this->field_id = new_record_parser->getRecordStructure()->getFieldIndexByName(this->field_name);
	
	// Note: if findFieldIndexByName fails above, the line below is not reached and the FieldParser remains
	// unbound.
	this->record_parser = const_cast<RecordParser*>(new_record_parser);
}

/* ====================================================================
 * class TimestampFieldParser - Parser for Timestamps
 * ====================================================================
 */

/** <summary>
 * TimestampFieldParser constructor.
 * </summary>
 * <remarks> Creates a parser for 64-bit integer timestamps. This FieldParser supports aggregating
 * multiple tokens. If, for example, the date and timestamp are two separate fields in
 * your input file, you can have this FieldParser aggregate the fields into one for parsing.
 * When fields are aggregated, they are joined with a space. The timestamp will be interpreted 
 * as being in GMT, with no leap seconds. This method uses the Boost library for date parsing.
 * Format tokens are similar, but not identical, to the UNIX function strptime. For a complete
 * list of tokens, see http://www.crystalclearsoftware.com/libraries/date_time/release_1_33/date_time/date_time_io.html#date_time.format_flags
 * </remarks>
 * <param name="new_consume_tokens">A vector of token numbers to be joined (with spaces) then parsed
 * into a timestamp</param>
 * <param name="new_format">A format string to use to parse the timestamp</param>
 * <param name="new_field_name">The field name to save the timestamp to when bound to a RecordParser</param>
 */
TimestampFieldParser::TimestampFieldParser(std::vector<size_t> new_consume_tokens,
										   std::string new_format,
										   std::string new_field_name) {
	using namespace std;											  
	this->consume_tokens = new_consume_tokens;
	this->format = new_format;
	this->field_name = new_field_name;
	this->missing_tokens_ok = false;

	this->locale_format = locale(locale::classic(), 
		new boost::posix_time::time_input_facet(this->format));
	this->epoch = boost::posix_time::ptime(boost::gregorian::date(1970,1,1),
		boost::posix_time::time_duration(0,0,0,0));
}

/** <summary>
 * Parses a vector of token strings, and writes the result to a record.
 * </summary>
 * <param name="tokens">A vector of token strings</param>
 * <param name="record">A pointer to a already-allocated block of memory to save the record to.</param>
 */
void TimestampFieldParser::writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record) {
	using namespace std;

	if(this->record_parser == NULL) {
		throw(FieldParserException("not bound to record parser"));
	}

	string token_string = "";
	boost::posix_time::ptime pt;

	for(size_t i = 0;i<this->consume_tokens.size();i++) {
		if(consume_tokens.at(i) >= tokens.size() && this->missing_tokens_ok) {
			token_string = token_string + " " + this->missing_token_replacement;
		} else {
			// note: this will throw an exception if missing_tokens_ok = false and 
			// the token to be consumed is out of bounds
			token_string = token_string + " " + tokens.at(consume_tokens.at(i));
		}
	}

	/* Now, the actual parsing of the timestamp string */
	istringstream is(token_string);
	is.imbue(locale_format);
	is >> pt;

	timestamp_t timestamp = (pt - this->epoch).total_milliseconds();
	
	// Write the timestamp to the record
	this->record_parser->getRecordStructure()->setMember(record,this->field_id,&timestamp);
}


/* ====================================================================
 * class StringFieldParser - Parser for strings
 * ====================================================================
 */

/** <summary>
 * StringFieldParser constructor.
 * </summary>
 * <remarks> 
 * </remarks>
 * <param name="new_consume_tokens">A vector of token numbers to be spliced.</param>
 * <param name="new_field_name">The field name to save the timestamp to when bound to a RecordParser</param>
 */
StringFieldParser::StringFieldParser(std::vector<size_t> new_consume_tokens,
										   std::string new_field_name) {
	using namespace std;											  
	this->consume_tokens = new_consume_tokens;
	this->field_name = new_field_name;
	this->missing_tokens_ok = false;
}

/** <summary>
 * Parses a vector of token strings, and writes the result to a record.
 * </summary>
 * <param name="tokens">A vector of token strings</param>
 * <param name="record">A pointer to a already-allocated block of memory to save the record to.</param>
 */
void StringFieldParser::writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record) {
	using namespace std;

	if(this->record_parser == NULL) {
		throw(FieldParserException("not bound to record parser"));
	}

	string token_string;
	token_string = tokens.at(consume_tokens.at(0));

	for(size_t i = 1;i<this->consume_tokens.size();i++) {
		if(consume_tokens.at(i) >= tokens.size() && this->missing_tokens_ok) {
			token_string = token_string + " " + this->missing_token_replacement;
		} else {
			// note: this will throw an exception if missing_tokens_ok = false and 
			// the token to be consumed is out of bounds
			token_string = token_string + " " + tokens.at(consume_tokens.at(i));
		}
	}

	// create some scratch memory to put the string in
	size_t size = this->record_parser->getRecordStructure()->getSizeOfField(this->field_id);
	char* tmpstr = new char[size];
	memset(tmpstr,0,size);

	// use the min of token_string.length() or the size of the field
	size = (token_string.length() > size ? size : token_string.length());
	memcpy(tmpstr,token_string.c_str(),size);
	
	// Write the timestamp to the record
	this->record_parser->getRecordStructure()->setMember(record,this->field_id,tmpstr);

	delete[] tmpstr;
}

/* ====================================================================
 * class Int32FieldParser - Parser for 32 Bit Integers
 * ====================================================================
 */

/** <summary>
 * Int32FieldParser constructor.
 * </summary>
 * <remarks> Creates a parser for 32-bit signed integers. This FieldParser only supports parsing one
 * token (no aggregation of tokens).
 * </remarks>
 * <param name="token">The token number to parse</param>
 * <param name="new_field_name">The field name to save the token when bound to a RecordParser</param>
 */
Int32FieldParser::Int32FieldParser(size_t token, std::string new_field_name) {
	using namespace std;											  
	this->consume_token = token;
	this->field_name = new_field_name;
	this->missing_tokens_ok = false;
}

/** <summary>
 * Parses a vector of token strings, and writes the result to a record.
 * </summary>
 * <param name="tokens">A vector of token strings</param>
 * <param name="record">A pointer to a already-allocated block of memory to save the record to.</param>
 */
void Int32FieldParser::writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record) {
	using namespace std;

	if(this->record_parser == NULL) {
		throw(FieldParserException("not bound to record parser"));
	}


	/* Now, the actual parsing of the string */
	tsdb::int32_t int32;
	if(this->missing_tokens_ok) {
		if(this->consume_token >= tokens.size()) {
			int32 = atol(this->missing_token_replacement.c_str());
		} else {
			int32 = atol(tokens.at(this->consume_token).c_str());
		}
	} else {
		int32 = atol(tokens.at(this->consume_token).c_str());
	}
	
	// Write the timestamp to the record
	this->record_parser->getRecordStructure()->setMember(record,this->field_id,&int32);
}

/* ====================================================================
 * class Int8FieldParser - Parser for 8 Bit Integers
 * ====================================================================
 */

/** <summary>
 * Int8FieldParser constructor.
 * </summary>
 * <remarks> Creates a parser for 8-bit signed integers. This FieldParser only supports parsing one
 * token (no aggregation of tokens).
 * </remarks>
 * <param name="token">The token number to parse</param>
 * <param name="new_field_name">The field name to save the token when bound to a RecordParser</param>
 */
Int8FieldParser::Int8FieldParser(size_t token, std::string new_field_name) {
	using namespace std;											  
	this->consume_token = token;
	this->field_name = new_field_name;
	this->missing_tokens_ok = false;
}

/** <summary>
 * Parses a vector of token strings, and writes the result to a record.
 * </summary>
 * <param name="tokens">A vector of token strings</param>
 * <param name="record">A pointer to a already-allocated block of memory to save the record to.</param>
 */
void Int8FieldParser::writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record) {
	using namespace std;

	if(this->record_parser == NULL) {
		throw(FieldParserException("not bound to record parser"));
	}


	/* Now, the actual parsing of the string */
	tsdb::int8_t int8;
	int integer;
	if(this->missing_tokens_ok) {
		if(this->consume_token >= tokens.size()) {
			integer = atol(this->missing_token_replacement.c_str());
		} else {
			integer = atol(tokens.at(this->consume_token).c_str());
		}
	} else {
		integer = atol(tokens.at(this->consume_token).c_str());
	}

	if(integer > 127 || integer < -127) {
		throw(FieldParserException("Integer out of bounds."));
	}

	int8 = (tsdb::int8_t) integer;
	
	// Write the timestamp to the record
	this->record_parser->getRecordStructure()->setMember(record,this->field_id,&int8);
}


/* ====================================================================
 * class CharFieldParser - Parser for 8-bit characters
 * ====================================================================
 */

/** <summary>
 * CharParser constructor.
 * </summary>
 * <remarks> Creates a parser for 8-bit characters. This FieldParser only supports parsing one
 * token (no aggregation of tokens).
 * </remarks>
 * <param name="token">The token number to parse</param>
 * <param name="new_field_name">The field name to save the token when bound to a RecordParser</param>
 */
CharFieldParser::CharFieldParser(size_t token, std::string new_field_name) {
	using namespace std;											  
	this->consume_token = token;
	this->field_name = new_field_name;
	this->missing_tokens_ok = false;
}

/** <summary>
 * Parses a vector of token strings, and writes the result to a record.
 * </summary>
 * <param name="tokens">A vector of token strings</param>
 * <param name="record">A pointer to a already-allocated block of memory to save the record to.</param>
 */
void CharFieldParser::writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record) {
	using namespace std;

	if(this->record_parser == NULL) {
		throw(FieldParserException("not bound to record parser"));
	}


	/* Now, the actual parsing of the string */
	const char* charptr;
	if(this->missing_tokens_ok) {
		if(this->consume_token >= tokens.size()) {
			charptr = this->missing_token_replacement.c_str();
		} else {
			charptr = tokens.at(this->consume_token).c_str();
		}
	} else {
		charptr = tokens.at(this->consume_token).c_str();
	}

	
	// Write the timestamp to the record
	this->record_parser->getRecordStructure()->setMember(record,this->field_id,charptr);
}


/* ====================================================================
 * class DoubleFieldParser - Parser for 64-bit IEEE Floating Points
 * ====================================================================
 */

/** <summary>
 * DoubleFieldParser constructor.
 * </summary>
 * <remarks><p>Creates a parser for 64-bit IEEE floating point numbers.
 * This FieldParser only supports parsing one
 * token (no aggregation of tokens).</p>
 * <p>IEEE floating points support a number of special values (infinities or nan).
 * This function uses the system's atof() to parse the token strings, so it is dependent 
 * on it to correctly parse these special values. The only special case which is handled directly
 * is if the token string is null or all spaces. In this case, the value stored is a quiet NaN.</p>
 * </remarks>
 * <param name="token">The token number to parse</param>
 * <param name="new_field_name">The field name to save the token when bound to a RecordParser</param>
 */
DoubleFieldParser::DoubleFieldParser(size_t token, std::string new_field_name) {
	using namespace std;											  
	this->consume_token = token;
	this->field_name = new_field_name;
	this->missing_tokens_ok = false;
}

/** <summary>
 * Parses a vector of token strings, and writes the result to a record.
 * </summary>
 * <param name="tokens">A vector of token strings</param>
 * <param name="record">A pointer to a already-allocated block of memory to save the record to.</param>
 */
void DoubleFieldParser::writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record) {
	using namespace std;
	string this_str;
	if(this->record_parser == NULL) {
		throw(FieldParserException("not bound to record parser"));
	}


	/* Now, the actual parsing of the string */
	tsdb::ieee64_t ieee64;

	if(this->missing_tokens_ok) {
		if(this->consume_token >= tokens.size()) {
			this_str = this->missing_token_replacement;
		} else {
			this_str = tokens.at(this->consume_token);
		}
	} else {
		this_str = tokens.at(this->consume_token);
	}

	tsdb::RecordParser::trim(this_str);
	
	if(this_str == "") {
		ieee64 = std::numeric_limits<double>::quiet_NaN();
	} else {
		ieee64 = atof(tokens.at(this->consume_token).c_str());
	}
	
	// Write the timestamp to the record
	this->record_parser->getRecordStructure()->setMember(record,this->field_id,&ieee64);
}



} //namespace tsdb




