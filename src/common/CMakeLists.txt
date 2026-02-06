add_library(
  ducklake_common OBJECT
  ducklake_data_file.cpp ducklake_name_map.cpp ducklake_types.cpp
  ducklake_util.cpp parquet_file_scanner.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ducklake_common>
    PARENT_SCOPE)
