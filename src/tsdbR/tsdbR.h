#ifndef TSDBR_H_
#define TSDBR_H_

#include <Rcpp.h>
#include "hdf5.h"
#include "structure.h"
#include "timeseries.h"
#include "tsdb.h"
#include "cell.h"
#include "recordset.h"
#include "record.h"
#include "field.h"
#include "table.h"

#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>

RcppExport SEXP TSDBopen();
RcppExport SEXP TSDBclose();
RcppExport SEXP TSDBopen_file(SEXP _filename, SEXP _permission);
RcppExport SEXP TSDBclose_file(SEXP _fid);
RcppExport SEXP TSDBtimeseries(SEXP _fid);
RcppExport SEXP TSDBget_properties(SEXP _goupID, SEXP _seriesName);
RcppExport SEXP TSDBget_records(SEXP _groupID, SEXP _timeseriesName,
		   SEXP _startTimestamp, SEXP _endTimestamp, SEXP _columnsWanted);
RcppExport SEXP TSDBcreate_file(SEXP _fileName, SEXP _permission);
RcppExport SEXP TSDBcreate_timeseries(SEXP _groupID, SEXP _seriesName,
		   SEXP _seriesDescription, SEXP _columns);
RcppExport SEXP TSDBappend(SEXP _groupID, SEXP _seriesName, SEXP _data,
		   SEXP _discardOverlap);
#endif /* TSDBR_H_ */
