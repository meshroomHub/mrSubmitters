cmake_minimum_required(VERSION 3.15)


set(PROJECT_NAME "mrSubmitters")
set(PROJECT_VERSION "0.1.0")
project(${PROJECT_NAME} VERSION ${PROJECT_VERSION} LANGUAGES NONE)


install(DIRECTORY "meshroom/"
    DESTINATION "${CMAKE_INSTALL_PREFIX}/meshroom"
    PATTERN "*.pyc" EXCLUDE
    PATTERN "__pycache__" EXCLUDE
)

install(DIRECTORY "config/"
    DESTINATION "${CMAKE_INSTALL_PREFIX}/config"
    PATTERN "*.pyc" EXCLUDE
    PATTERN "__pycache__" EXCLUDE
)

install(DIRECTORY "python/"
    DESTINATION "${CMAKE_INSTALL_PREFIX}/python"
    PATTERN "*.pyc" EXCLUDE
    PATTERN "__pycache__" EXCLUDE
)

install(DIRECTORY "script/"
    DESTINATION "${CMAKE_INSTALL_PREFIX}/script"
    PATTERN "*.pyc" EXCLUDE
    PATTERN "__pycache__" EXCLUDE
)
