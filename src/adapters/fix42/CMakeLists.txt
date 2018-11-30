file(GLOB_RECURSE SRC_FILES *.cc)
add_library(fix42 MODULE ${SRC_FILES})
target_link_libraries(fix42 ${QUICKFIX_LIBRARY_PATH})
