add_library(
  ducklake_functions OBJECT
  base_metadata_function.cpp
  ducklake_add_data_files.cpp
  ducklake_cleanup_old_files.cpp
  ducklake_expire_snapshots.cpp
  ducklake_flush_inlined_data.cpp
  ducklake_current_snapshot.cpp
  ducklake_last_committed_snapshot.cpp
  ducklake_list_files.cpp
  ducklake_merge_adjacent_files.cpp
  ducklake_set_commit_message.cpp
  ducklake_set_option.cpp
  ducklake_snapshots.cpp
  ducklake_options.cpp
  ducklake_table_changes.cpp
  ducklake_table_info.cpp
  ducklake_table_insertions.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ducklake_functions>
    PARENT_SCOPE)
