#pragma once

#include <vector>
#include <string>
#include <locale>

#include "boost/date_time.hpp"
#include "tsdb.h"

/* Forward Declarations */
namespace tsdb {
	class RecordParser;
}


namespace tsdb {



/* ------------------------------------------------------------------
 * FieldParserException. For runtime errors thrown by the FieldParser 
 * class.
 * ------------------------------------------------------------------
 */
class  FieldParserException: 
	public std::runtime_error 
{ 
public:

	FieldParserException(const std::string& what):
	  std::runtime_error(std::string("FieldParserException: ") + what) {} 

};

class  FieldParser
{
public:

	/* Methods */
	virtual void writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record);
	void bindToRecordParser(tsdb::RecordParser* new_record_parser);

	~FieldParser(void);
protected:
	FieldParser(void);
	
	RecordParser* record_parser;

	std::string field_name;
	std::string format;

	size_t field_id;

};

class  TimestampFieldParser : public FieldParser
{
public:
	/* Constructor */
	TimestampFieldParser(std::vector<size_t> new_consume_tokens, std::string new_format,
		std::string new_field_name);

	/* Other Methods */
	void writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record);

private:
	std::locale	locale_format;
	boost::posix_time::ptime epoch;
	std::vector<size_t> consume_tokens;

};

class  DoubleFieldParser : public FieldParser
{
public:
	/* Constructor */
	DoubleFieldParser(size_t new_consume_token,
		std::string new_field_name);

	/* Other Methods */
	void writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record);

private:
	size_t consume_token;

};

class  Int32FieldParser : public FieldParser
{
public:
	/* Constructor */
	Int32FieldParser(size_t new_consume_token,
		std::string new_field_name);

	/* Other Methods */
	void writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record);

private:
	size_t consume_token;

};

class  Int8FieldParser : public FieldParser
{
public:
	/* Constructor */
	Int8FieldParser(size_t new_consume_token,
		std::string new_field_name);

	/* Other Methods */
	void writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record);

private:
	size_t consume_token;

};


class  CharFieldParser : public FieldParser
{
public:
	/* Constructor */
	CharFieldParser(size_t new_consume_token,
		std::string new_field_name);

	/* Other Methods */
	void writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record);

private:
	size_t consume_token;

};

class  StringFieldParser : public FieldParser
{
public:
	/* Constructor */
	StringFieldParser(std::vector<size_t> new_consume_tokens,
		std::string new_field_name);

	/* Other Methods */
	void writeParsedTokensToRecord(const std::vector<std::string> &tokens, void* record);

private:
	std::vector<size_t> consume_tokens;

};


}
