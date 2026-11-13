add_subdirectory(common)
add_subdirectory(functions)
add_subdirectory(metadata_manager)
add_subdirectory(rest)
add_subdirectory(storage)

add_library(ducklake_library OBJECT ducklake_extension.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ducklake_library>
    PARENT_SCOPE)
