#pragma once

void TSDBopen(void);
void TSDBclose(void);
int TSDBopen_file(const char * filename, const char * permission);
int TSDBclose_file(int fid); 
int TSDBcreate_file(const char * filename);
void TSDBcreate_timeseries(int fid, const char * seriesname, const char * desc,
					char * columnTypes[], char * columnNames[],int numColumns);
mxArray* TSDBread_timeseries_by_timestamp(int loc_id, const char * series, long long start_ts, long long end_ts);
mxArray* TSDBget_timeseries_info(int loc_id,const char * series);
int TSDBtimeseries_append(int loc_id, const char * series, void * data);
mxArray* TSDBget_timeseries_names(int loc_id);
