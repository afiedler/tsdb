TSDB_SOURCES = field.cpp cell.cpp record.cpp recordset.cpp structure.cpp  \
			   table.cpp timeseries.cpp bufferedrecordset.cpp memoryblock.cpp \
			   memoryblockptr.cpp			   
TSDB_OBJECTS = $(TSDB_SOURCES:.cpp=.o)
TSDB = ./../tsdb
BOOST = C:/projectTools/boost/boost_1_47_0-mingw/boost_1_47_0
HDF5 = C:/projectTools/hdf5/hdf5-1.8.7-mingw/hdf5lib/release
RCPP = C:/projectTools/R/R-2.13.1/library/Rcpp
R = C:/projectTools/R/R-2.13.1

CPPC = g++
CPPFLAGS = -O2 -D_GCC_ -fPIC -c

all: tsdbR.o  $(TSDB_OBJECTS) 
	g++ -O3 -D_GCC_ -shared -o libtsdbR.dll tsdbR.o $(TSDB_OBJECTS) \
	-L$(RCPP)/lib/i386 -lRcpp \
	-L$(R)/bin/i386 -lR \
	-L$(HDF5)/lib -l:libhdf5.a -l:libhdf5_hl.a \
	-L$(BOOST)/stage/lib -l:libboost_date_time-mgw45-1_47.a
	
tsdbR.o : tsdbR.cpp
	$(CPPC) $(CPPFLAGS) -I$(TSDB) -I$(BOOST) -I$(HDF5)/include \
	-I$(R)/include -I$(RCPP)/include $<
	
%.o : $(TSDB)/%.cpp
	$(CPPC) $(CPPFLAGS) $(TSDB)/$< -I$(HDF5)/include -I$(BOOST) 
	
clean:
	rm *.o *.so



