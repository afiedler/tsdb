solution "TSDB"
  configurations { "Debug", "Release" }
 
  configuration { "Debug" }
    targetdir "bin/debug"
 
  configuration { "Release" }
    targetdir "bin/release"
 
  if _ACTION == "clean" then
    os.rmdir("bin")
  end

  project "tsdb"
  	language "C++"
  	kind "SharedLib"
  	files  { "src/tsdb/*.h", "src/tsdb/*.cpp" }
  	links { "hdf5", "hdf5_hl", "boost_system-mt", "boost_date_time-mt" }
 
  configuration { "Debug*" }
    defines { "_DEBUG", "DEBUG" }
    flags   { "Symbols" }
 
  configuration { "Release*" }
    defines { "NDEBUG" }
    flags   { "Optimize" }