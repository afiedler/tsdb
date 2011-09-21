#include "tokenfilter.h"

namespace tsdb {

/* ------------------------------------------------------------------
 * TokenFilterException. For runtime errors thrown by the TokenFilter 
 * class.
 * ------------------------------------------------------------------
 */
class TokenFilterException: 
	public std::runtime_error 
{ 
public:

	TokenFilterException(const std::string& what):
	  std::runtime_error(std::string("TokenFilterException: ") + what) {} 

};

/* ====================================================================
 * class TokenFilter - Checks tokens against a simple boolean filter
 * ====================================================================
 */

/** <summary>Constructs a new TokenFilter</summary>
 * <remarks>Creates a TokenFilter. A TokenFilter takes a set of tokens, joins them by spaces,
 * and compares the resulting string to a constant string. Based on this, a RecordParser can
 * decide to discard a record before even parsing the underlying values.
 * </remarks>
 * <param name="new_apply_to_tokens">A vector of token index numbers to join into a string</param>
 * <param name="new_compare_operator">A Comparison enum to be used</param>
 * <param name="new_compare_to">A string to compare the joined tokens to</param>
 */
TokenFilter::TokenFilter(std::vector<size_t> new_apply_to_tokens, tsdb::TokenFilter::Comparison new_compare_operator, std::string new_compare_to) {
	/* Check the input arguments for sanity */
	if(new_apply_to_tokens.size() == 0) {
		throw(TokenFilterException("no tokens specified to use"));
	}

	/* Set object properties */
	this->apply_to_tokens = new_apply_to_tokens;
	this->compare_operator = new_compare_operator;
	this->compare_to = new_compare_to;
}

/** <summary>Evaluates the filter on a vector of tokens</summary>
 * <remarks>Returns <c>true</c> if the joined token string compared to this->compare_to is
 * true, and false otherwise (using the predefined comparison operator).
 * </remarks>
 * <param name="tokens">A vector of token strings</param>
 */
bool TokenFilter::evaluateFilterOnTokens(const std::vector<std::string> &tokens) {
	using namespace std;

	bool retval = true;


	/* Combine string to evaluate, if applied to more than one token */
	if (this->apply_to_tokens.size() > 1) {
		string eval_str = "";
		size_t ntokens = tokens.size();
		for(size_t i = 0; i<this->apply_to_tokens.size(); ) {
			if(ntokens <= this->apply_to_tokens.at(i)) {
				throw(TokenFilterException("not enough tokens in token array to process filter"));
			}

			eval_str = eval_str + tokens.at(this->apply_to_tokens.at(i));
			i++;
			if(i<this->apply_to_tokens.size()) {
				eval_str = eval_str + " ";
			}
		}

		
		/* Use the comparison operator to evaluate the string */

		switch(this->compare_operator) {
			case EQUAL_TO:
				if(eval_str == this->compare_to) {
					retval = true;
				} else {
					retval = false;
				}
				break;

			case NOT_EQUAL_TO:
				if(eval_str != this->compare_to) {
					retval = true;
				} else {
					retval = false;
				}
				break;
		}
	} else {
		/* Use the comparison operator to evaluate the string */

		switch(this->compare_operator) {
			case EQUAL_TO:
				if(tokens.at(this->apply_to_tokens.at(0)).compare(this->compare_to) == 0) {
					retval = true;
				} else {
					retval = false;
				}
				break;

			case NOT_EQUAL_TO:
				if(tokens.at(this->apply_to_tokens.at(0)).compare(this->compare_to) != 0) {
					retval = true;
				} else {
					retval = false;
				}
				break;
		}
	}

	return retval;

}

} // namespace tsdb