list(APPEND source_ddckv 
    nm.cc 
    ib.cc 
    server_mm.cc 
    server.cc
    kv_utils.cc
    hashtable.cc
    client_mm.cc
    client.cc
    client_cr.cc)

add_library(libddckv STATIC ${source_ddckv})

link_directories(lib)
add_library(mralloc STATIC IMPORTED)
set_target_properties(mralloc PROPERTIES IMPORTED_LOCATION ${PROJECT_SOURCE_DIR}/lib/libmralloc.a)

target_compile_options(
    libddckv
    PRIVATE
    # "-O2"
    # ${CMAKE_CXX_FLAGS_DEBUG}
    "-gdwarf-4"
    "-g"
    "-D_DEBUG"
)

target_link_libraries(libddckv mralloc ${Boost_LIBRARIES} boost_context boost_fiber tbb rdmacm ibverbs rt )

