solution "TSDB"
  configurations { "Debug", "Release" }
 
  configuration { "Debug" }
    targetdir "bin/debug"
 
  configuration { "Release" }
    targetdir "bin/release"
 
  if _ACTION == "clean" then
    os.rmdir("bin")
  end

 
  configuration { "Debug*" }
    defines { "_DEBUG", "DEBUG" }
    flags   { "Symbols" }
 
  configuration { "Release*" }
    defines { "NDEBUG" }
    flags   { "Optimize" }

  project "ticpp"
    -- Making this as a static lib
    kind          "StaticLib"
    -- Set the files to include/exclude.
    files           { "src/ticpp/*.cpp", "src/ticpp*.h" }
    excludes          { "xmltest.cpp" }

    -- Set the defines.
    defines           { "TIXML_USE_TICPP" }

    -- Common setup
    language          "C++"
    flags           { "ExtraWarnings" }


  project "tsdb"
    language "C++"
    kind "SharedLib"
    files  { "src/tsdb/*.h", "src/tsdb/*.cpp" }
    links { "hdf5", "hdf5_hl", "boost_system-mt", "boost_date_time-mt" }

  project "tsdbcreate"
    language "C++"
    kind "ConsoleApp"
    files  { "src/tsdbcreate/*.h", "src/tsdbcreate/*.cpp" }
    includedirs { "src/tsdb" }
    links { "tsdb", "hdf5", "hdf5_hl", "boost_system-mt", "boost_date_time-mt" }

  project "tsdbimport"
    language "C++"
    kind "ConsoleApp"
    files  {  "src/tsdbimport/*.h", "src/tsdbimport/*.cpp" }
    includedirs { "src/tsdb", "src/ticpp" }
    links { "tsdb", "hdf5", "hdf5_hl", "boost_system-mt", "boost_date_time-mt", "ticpp" }

  project "tsdbview"
    language "C++"
    kind "ConsoleApp"
    files  { "src/tsdbview/*.h", "src/tsdbview/*.cpp" }
    includedirs { "src/tsdb" }
    links { "tsdb", "hdf5", "hdf5_hl", "boost_system-mt", "boost_date_time-mt" }
 