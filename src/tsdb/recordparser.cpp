/* STL includes */
#include <string>
#include <sstream>
#include <vector>

/* TSDB includes */
#include "fieldparser.h"
#include "tokenfilter.h"
#include "structure.h"

#include "recordparser.h"

namespace tsdb {

/* ---------------------------------------------------------------------
 * RecordParserException. For runtime errors thrown by the RecordParser 
 * class.
 * ---------------------------------------------------------------------
 */
class RecordParserException: 
	public std::runtime_error 
{ 
public:

	RecordParserException(const std::string& what):
	  std::runtime_error(std::string("RecordParserException: ") + what) {} 

};

/* ====================================================================
 * class RecordParser - creates a parser for one record
 * ====================================================================
 */

/** <summary>
 * Creates a RecordParser to parse a line or vector of tokens into a 
 * Structure that represents a record.
 * </summary>
 */
RecordParser::RecordParser(void) {
	this->record_struct = NULL;
	this->delim = ",";
	this->esc = "\\";
	this->quote = "\"'";
	this->simple_parse = false;
}

RecordParser::~RecordParser(void) {
}

/** <summary>
 * Adds a FieldParser to the RecordParser.
 * </summary>
 * <remarks>
 * FieldParsers are executed in the order that they are added to the record parser
 * and <em>after</em> any TokenFilters. You must have linked the RecordParser to a
 * Structure before calling this function, otherwise a RecordParserException will be
 * thrown. You may do the linking by calling RecordParser::setRecordStruture().
 * </remarks>
 * <param name="new_field_parser">Pointer to a FieldParser to add.</param>
 */
void RecordParser::addFieldParser(FieldParser* new_field_parser) {
	if(this->record_struct == NULL) {
		throw(RecordParserException("cannot add field parser because the RecordParser is not linked to a structure"));
	}
	this->field_parsers.push_back(new_field_parser);
	new_field_parser->bindToRecordParser(this);
}

/** <summary>
 * Adds a TokenFilter to the RecordParser.
 * </summary>
 * <remarks>
 * TokenFilters are executed in the order that they are added to the record parser
 * and <em>before</em> any FieldParsers. You do not have to link a Structure to the
 * RecordParser before adding TokenFilters.
 * </remarks>
 * <param name="new_token_filter">Pointer to a TokenFilter to add.</param>
 */
void RecordParser::addTokenFilter(TokenFilter* new_token_filter) {
	this->token_filters.push_back(new_token_filter);
}

/** <summary>
 * Links the RecordParser to a Structure.
 * </summary>
 * <remarks>
 * <p>The RecordParser needs a Structure that describes the underlying memory layout of the
 * data structure it is coercing tokens into. This function sets that link.</p>
 * <p>This method will call FieldParser::bindToRecordParser() for each of the field parsers have
 * been added to the RecordParser to update the FieldParsers as to the new Structure. In this way,
 * you may call this function to change the structure the RecordParser is using <em>after</em>
 * FieldParsers have been added.</p>
 * </remarks>
 * <param name="new_record_struct">Pointer to a Structure to link the RecordParser to.</param>
 */
void RecordParser::setRecordStructure(Structure* new_record_struct) {
	
	/* Call each of the field parsers */
	this->record_struct = new_record_struct;

	try {
		for(size_t i = 0; i<this->field_parsers.size(); i++) {
			this->field_parsers.at(i)->bindToRecordParser(this);
		}
	} catch(...) {
		this->record_struct = NULL;
		throw;
	}
}

/** <summary>
 * Returns a pointer to the Structure used, otherwise returns NULL.
 * </summary>
 */
Structure* RecordParser::getRecordStructure() {
	return this->record_struct;
}

/** <summary>
 * Returns a pointer to a block of memory with the record created by parsing tokens.
 * </summary>
 * <remarks>
 * <p>This function will take a vector of strings called "tokens" and first call
 * the TokenFilters to decide if the tokens should, in fact, be parsed. If no
 * TokenFilter returns <c>true</c>, then it calls all of the FieldParsers to 
 * parse the tokens down to a record.</p>
 * <p>If a record is successfully parsed (and is not filtered by the token filters)
 * this function allocates memory with <c>malloc()</c>, writes the record to this memory
 * and returns the pointer. <em>You must take care to deallocate this memory</em>.
 * If a token filter evaluates to <c>true</c>, NULL is returned 
 * and the record is not parsed (no memory allocated). If there is any parsing exception, the memory for the
 * record is deallocated and this method rethrows the exception. You do not need to deallocate anything in this
 * case.</p>
 * </remarks>
 * <param name="tokens">A vector of token strings</param>
 */
void* RecordParser::parseTokens(const std::vector<std::string> &tokens) {
	size_t i;
	
	if(this->record_struct == NULL) {
		throw(RecordParserException("not bound to structure"));
	}

	// Use the TokenFilters to filter the record out before any of the
	// tokens are parsed into data points.
	for(i=0; i<this->token_filters.size(); i++) {
		// If the TokenFilter evaluates to true, then the record is excluded.
		if(this->token_filters.at(i)->evaluateFilterOnTokens(tokens)) {
			return NULL;
		}
	}

	void* record = NULL;
	record = malloc(this->record_struct->getSizeOf());
	if(record == NULL) {
		throw(RecordParserException("out of memory"));
	}

	memset(record, 0, this->record_struct->getSizeOf());

	/* Call each of the FieldParsers */
	try {
		for(i = 0; i<this->field_parsers.size(); i++) {
			this->field_parsers.at(i)->writeParsedTokensToRecord(tokens,record);
		}
	} catch(...) {
		free(record);
		throw;
	}

	return record;

}

/** <summary>
 * Parses tokens and saves to a block of memory.
 * </summary>
 * <remarks>
 * <p>This function will take a vector of strings called "tokens" and first call
 * the TokenFilters to decide if the tokens should, in fact, be parsed. If no
 * TokenFilter returns <c>true</c>, then it calls all of the FieldParsers to 
 * parse the tokens down to a record.</p>
 * <p>If a record is successfully parsed (and is not filtered by the token filters),
 * this function writes the record to the memory pointed to by record.
 * <em>You must take care to allocate an appropriate amount of memory</em>.
 * If a token filter evaluates to <c>true</c>, nothing is written to memory, and false is returned.
 * If there is any parsing exception, this method rethrows the exception. Memory may be changed in that case.</p>
 * </remarks>
 * <param name="tokens">A vector of token strings</param>
 * <param name="record">Pointer to meoory for a record</param>
 */
bool RecordParser::parseTokens(const std::vector<std::string> &tokens, void* record) {
	size_t i;
	
	if(this->record_struct == NULL) {
		throw(RecordParserException("not bound to structure"));
	}

	// Use the TokenFilters to filter the record out before any of the
	// tokens are parsed into data points.
	for(i=0; i<this->token_filters.size(); i++) {
		// If the TokenFilter evaluates to true, then the record is excluded.
		if(this->token_filters.at(i)->evaluateFilterOnTokens(tokens)) {
			return false;
		}
	}


	memset(record, 0, this->record_struct->getSizeOf());

	/* Call each of the FieldParsers */
	try {
		for(i = 0; i<this->field_parsers.size(); i++) {
			this->field_parsers.at(i)->writeParsedTokensToRecord(tokens,record);
		}
	} catch(...) {
		throw;
	}

	return true;

}

/** <summary>
 * Sets the field delimiter to use when parsing a string into a record.
 * </summary>
 * <remarks>
 * Note that the default delimiter is a comma. You may specify more than one delimiter, 
 * the parser simply uses any character in the <c>new_delim</c> string as a delimiter.
 * </remarks>
 * <param name="new_delim">A string representing the new delimiter(s) to use.</param>
 */
void RecordParser::setDelimiter(std::string new_delim) {
	this->delim = new_delim;
}

/** <summary>
 * Sets whether to use the simple or more advanced (slower) parser.
 * </summary>
 * <remarks>
 * </remarks>
 * <param name="_simple_parse">true for simple, false for advanced</param>
 */
void RecordParser::setSimpleParse(bool _simple_parse) {
	this->simple_parse = _simple_parse;
}

/** <summary>
 * Sets the field escape character(s) to use when parsing a string into a record.
 * </summary>
 * <remarks>
 * Note that the default escape character is a forward slash. You may specify more than one, 
 * the parser simply uses any character in the <c>new_esc</c> string as a escape character. If you
 * specify an empty string, no escape character is used when parsing.
 * </remarks>
 * <param name="new_esc">A string representing the new escape character(s) to use.</param>
 */
void RecordParser::setEscapeCharacter(std::string new_esc) {
	this->esc = new_esc;
}

/** <summary>
 * Sets the field quote character(s) to use when parsing a string into a record.
 * </summary>
 * <remarks>
 * Note that the default quote characters are double and single quotes (" and '). You may specify more than one, 
 * the parser simply uses any character in the <c>new_esc</c> string as a quote character. You may also specify 
 * an empty string, in which case the parser will treat quotes as any other character.
 * </remarks>
 * <param name="new_quote">A string representing the new quote character(s) to use.</param>
 */
void RecordParser::setQuoteCharacter(std::string new_quote) {
	this->quote = new_quote;
}

/** <summary>
 * Parses a string to tokens, then parses the tokens to a record using the TokenFilters and
 * FieldParsers.
 * </summary>
 * <remarks>
 * <p>This function parses a line using a comma as a delimiter, forward slash as a escape character, and
 * either double or single quotes as quote characters. You can changes these defaults using 
 RecordParser::setDelimiter(), RecordParser::setEscapeCharacter() or RecordParser::setQuoteCharacter().
 Using the defaults, the following are valid lines:</p>
 * <ul>
 * <li>Token 1,Token 2,Token 3</li>
 * <li>Token 1,"Token 2, with comma",Token 3</li>
 * <li>Token 1,Token 2 with \"embedded quote\",Token 3</li>
 * <li>Token 1,Token 2 with \n newline,Token 3</li>
 * <li>Token 1,Token 2 with embedded \\,Token 3</li>
 * </ul>
 * <p>Note that this function tokenizes the line, then calls RecordParser::parseTokens() to convert the
 * tokens to a record. See that method to understand the return values. Note: If this function returns
 * a non-NULL pointer, you must <c>free()</c> this memory when you are finished with it. It is unmanaged 
 * by the TSDB library at that point (this is just like parseTokens()).</p>
 * </remarks>
 * <param name="line">A line to parse to tokens, then to a record.</param>
 */
void* RecordParser::parseString(const std::string &line) {
	using namespace std;
	using namespace boost;
	vector<string> tokens;

	escaped_list_separator<char> els(this->esc,this->delim,this->quote);
	tokenizer<escaped_list_separator<char> > tok(line,els); 
	
	for(tokenizer<escaped_list_separator<char> >::iterator beg=tok.begin();
		beg!=tok.end(); ++beg) {
		tokens.push_back(*beg);
	}


	return this->parseTokens(tokens);

}

/** <summary>
 * Parses a string to tokens, then parses the tokens to a record using the TokenFilters and
 * FieldParsers. Saves the result to the memory block pointed to by record.
 * </summary>
 * <remarks>
 * <p>This function is much faster than parseString() because it does not allow escape characters or
 * quoted tokens. It simply splits a string into tokens whenever it encounters a character in the delimiter
 * string. If your CSV is very basic, you should use this function. By default, the delimiter is a comma, 
 * use RecordParser::setDelimiter() to change it.</p>
 * <p>Note that this function tokenizes the line, then calls RecordParser::parseTokens() to convert the
 * tokens to a record. It then writes the result out to record.</p>
 * </remarks>
 * <param name="line">A line to parse to tokens, then to a record.</param>
 */
bool RecordParser::parseBasicString(const std::string &line, void * record) {
	using namespace std;
	/*using namespace boost;
	unsigned int counter = 0;

	char_separator<char> csep(this->delim.c_str(),"", boost::keep_empty_tokens);
	tokenizer<char_separator<char>> tok(line,csep);

	/*for(tokenizer<char_separator<char>>::iterator beg=tok.begin();
		beg!=tok.end(); ++beg) {
			if(this->tokenbuf.size() < ++counter) { this->tokenbuf.resize(counter); }
			this->tokenbuf[counter-1].assign(*beg);
	}

	this->tokenbuf.resize(counter);*/

	/*this->tokenbuf.assign(tok.begin(),tok.end());*/
	
	stringstream ss(line);
	unsigned int counter = 0;
	std::string item;

	while(std::getline(ss,item,',')) {
		if(counter < this->tokenbuf.size()) {
			this->tokenbuf[counter] = item;
		} else {
			this->tokenbuf.push_back(item);
		}
		counter++;
	}
	this->tokenbuf.resize(counter);

		
	return this->parseTokens(this->tokenbuf, record);

}

/** <summary>
 * Parses a string to tokens, then parses the tokens to a record using the TokenFilters and
 * FieldParsers. Saves the result to the memory block pointed to by record.
 * </summary>
 * <remarks>
 * <p>Note that this function tokenizes the line, then calls RecordParser::parseTokens() to convert the
 * tokens to a record. It then writes the result out to record.</p>
 * </remarks>
 * <param name="line">A line to parse to tokens, then to a record.</param>
 */
bool RecordParser::parseString(const std::string &line, void * record) {
	using namespace std;
	using namespace boost;

	if(!this->simple_parse) {
		escaped_list_separator<char> els(this->esc,this->delim,this->quote);
		tokenizer<escaped_list_separator<char> > tok(line,els); 
		this->tokenbuf.clear();
		for(tokenizer<escaped_list_separator<char> >::iterator beg=tok.begin();
			beg!=tok.end(); ++beg) {
				this->tokenbuf.push_back(*beg);
		}
	} else {
	
		stringstream ss(line);
		unsigned int counter = 0;
		std::string item;

		while(std::getline(ss,item,',')) {
			if(counter < this->tokenbuf.size()) {
				this->tokenbuf[counter] = item;
			} else {
				this->tokenbuf.push_back(item);
			}
			counter++;
		}
		this->tokenbuf.resize(counter);
	}

		
	return this->parseTokens(this->tokenbuf, record);

}

/** <summary>
 * Parses a string to tokens, then parses the tokens to a record using the TokenFilters and
 * FieldParsers.
 * </summary>
 * <remarks>
 * <p>This function is much faster than parseString() because it does not allow escape characters or
 * quoted tokens. It simply splits a string into tokens whenever it encounters a character in the delimiter
 * string. If your CSV is very basic, you should use this function. By default, the delimiter is a comma, 
 * use RecordParser::setDelimiter() to change it.</p>
 * <p>Note that this function tokenizes the line, then calls RecordParser::parseTokens() to convert the
 * tokens to a record. See that method to understand the return values. Note: If this function returns
 * a non-NULL pointer, you must <c>free()</c> this memory when you are finished with it. It is unmanaged 
 * by the TSDB library at that point (this is just like parseTokens()).</p>
 * </remarks>
 * <param name="line">A line to parse to tokens, then to a record.</param>
 */
void* RecordParser::parseBasicString(const std::string &line) {
	using namespace std;
	using namespace boost;
	unsigned int counter = 0;

	char_separator<char> csep(this->delim.c_str(),"", boost::keep_empty_tokens);
	tokenizer<char_separator<char> > tok(line,csep);

	/*for(tokenizer<char_separator<char>>::iterator beg=tok.begin();
		beg!=tok.end(); ++beg) {
			if(this->tokenbuf.size() < ++counter) { this->tokenbuf.resize(counter); }
			this->tokenbuf[counter-1].assign(*beg);
	}

	this->tokenbuf.resize(counter);*/

	this->tokenbuf.assign(tok.begin(),tok.end());



	return this->parseTokens(this->tokenbuf);

}

/** <summary>
 * Trims spaces from the beginning and ending of a string.
 * </summary>
 * <remarks>
 * This is a conveience function to trim a string. Some of the FieldParsers
 * make use of this function.
 * </remarks>
 * <param name="str">A reference to a string to trim</param>
 */
void RecordParser::trim(std::string& str) {
	using namespace std;
	string::size_type pos = str.find_last_not_of(' ');
	if(pos != string::npos) {
		str.erase(pos + 1);
		pos = str.find_first_not_of(' ');
		if(pos != string::npos) str.erase(0, pos);
		}
	else str.erase(str.begin(), str.end());
}

} // namespace tsdb
