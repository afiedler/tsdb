#pragma once

#include <vector>
#include <string>

#include "tsdb.h"

namespace tsdb {

/** <summary>Represents a means to filter unparsed tokens</summary>
 * <remarks><p>A TokenFilter represents a simple boolean expression where a set of 
 * tokens are joined into a string separated by spaces are compared to a constant 
 * string. The user can select the comparison operator (EQUAL_TO or NOT_EQUAL_TO).</p>
 * <p>When using the TokenFilter with a RecordParser, the RecordParser evaluates the TokenFilters
 * before any FieldParsers are called. This lets you filter out unsuitable records prior to
 * any expensive parsing operations (such as timestamp parsing). This can signifigantly improve
 * the performance of parsing large CSV files.</p>
 * <p>The RecordParser calls each TokenFilter, and if any TokenFilter evaluates to <c>true</c>, parsing is halted.
 * </p>
 * </remarks>
 */
class  TokenFilter
{
public:
	enum Comparison {
		EQUAL_TO,
		NOT_EQUAL_TO
	};
	
	/* Constructors */
	TokenFilter(std::vector<size_t> new_apply_to_tokens, Comparison new_compare_operator,
		std::string new_compare_to);

	/* Other Methods */
	bool evaluateFilterOnTokens(const std::vector<std::string> &tokens);

	/* Destructor */
	~TokenFilter(void);

private:
	std::vector<size_t> apply_to_tokens;
	Comparison compare_operator;
	std::string compare_to;

};

} //namespace tsdb