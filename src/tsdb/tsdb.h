#pragma once



 /* This is for Structure packing. The code will make structures
    that are packed to the appropriate word size. */
#define ARCH_WORD_SIZE 4

/* Size of the Table append buffer */
#define APPEND_BUFFER_SIZE 1000


/* Constants */
#ifndef SPLIT_INDEX_GT
	#define SPLIT_INDEX_GT 262144
#endif

#ifndef INDEX_STEP
	#define INDEX_STEP 65536
#endif



/* -----------------------------------------------------------------
 * Typedefs. Basic types used by the library.
 * -----------------------------------------------------------------
 */
typedef long long timestamp_t;
/* Debugger */
#ifdef _DEBUG
#include <iostream>
#define tracer if (0) ; else cout
#else
#define tracer if (1) ; else cout
#endif





