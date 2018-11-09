include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# SIMPLE_EXAMPLE
add_executable(SimpleExample simple_example.cpp)
target_link_libraries(SimpleExample csci5570)
target_link_libraries(SimpleExample ${HUSKY_EXTERNAL_LIB})
target_link_libraries(SimpleExample ${GTEST_LIBRARIES})
target_link_libraries(SimpleExample ${GMOCK_LIBRARIES})
set_property(TARGET SimpleExample PROPERTY CXX_STANDARD 11)
add_dependencies(SimpleExample ${external_project_dependencies})