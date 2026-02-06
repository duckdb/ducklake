add_library(ducklake_statistics OBJECT ducklake_geo_stats.cpp
                                       ducklake_variant_stats.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ducklake_statistics>
    PARENT_SCOPE)
