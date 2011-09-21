#pragma once

#include <vector>
#include "structure.h"
#include "tokenfilter.h"
#include "tsdb.h"


namespace tsdb {

/* Forward Declarations */
class FieldParser;


class  RecordParser 
{
public:
	RecordParser(void);

	void setRecordStructure(Structure* new_record_structure);
	Structure* getRecordStructure();
	void* parseTokens(const std::vector<std::string> &tokens);
	bool parseTokens(const std::vector<std::string> &tokens, void* record);
	void* parseString(const std::string &line);
	void* parseBasicString(const std::string &line);
	bool parseBasicString(const std::string &line, void * record);
	bool parseString(const std::string &line, void * record);
	void addFieldParser(tsdb::FieldParser*);
	void addTokenFilter(tsdb::TokenFilter*);
	void setDelimiter(std::string new_delim);
	void setEscapeCharacter(std::string new_esc);
	void setQuoteCharacter(std::string new_quote);
	void setSimpleParse(bool _simple_parse);
	static void trim(std::string& str);
	~RecordParser(void);
private:
	Structure* record_struct;
	std::vector<tsdb::FieldParser*> field_parsers;
	std::vector<tsdb::TokenFilter*> token_filters;
	bool simple_parse;
	std::string delim;
	std::string esc;
	std::string quote;
	std::vector<std::string> tokenbuf;
};

} // namespace tsdb;