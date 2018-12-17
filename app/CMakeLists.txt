include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# SIMPLE_EXAMPLE
add_executable(SimpleExample simple_example.cpp)
target_link_libraries(SimpleExample csci5570)
target_link_libraries(SimpleExample ${HUSKY_EXTERNAL_LIB})
target_link_libraries(SimpleExample ${GTEST_LIBRARIES})
target_link_libraries(SimpleExample ${GMOCK_LIBRARIES})
set_property(TARGET SimpleExample PROPERTY CXX_STANDARD 11)
add_dependencies(SimpleExample ${external_project_dependencies})

# LOGISTIC REGRESSION
add_executable(LogisticRegression logistic_regression.cpp)
target_link_libraries(LogisticRegression csci5570)
target_link_libraries(LogisticRegression ${HUSKY_EXTERNAL_LIB})
target_link_libraries(LogisticRegression ${GTEST_LIBRARIES})
target_link_libraries(LogisticRegression ${GMOCK_LIBRARIES})
set_property(TARGET LogisticRegression PROPERTY CXX_STANDARD 11)
add_dependencies(LogisticRegression ${external_project_dependencies})