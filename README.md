This is a project based on stanford cs245.

# Prerequisite
- cmake version(recommended): >=3.9.2
- C++ version: >=C++11
- gcc/g++ version: >=6.3.0

# Step 1
Configure googletest framework in your environment.

`sh configure.sh`

After configuration, binary files of google test will be installed in your environment.

# Step 2
Implement your database in C++, then build binary outputs (executables for unit test).

If you are working with lab1, you can build unit tests only for lab1,

`sh build.sh --clean --lab1`

or for lab2,

`sh build.sh --clean --lab2`

or for both labs

`sh build.sh --clean --lab1 --lab2`

# Step 3
Enter the test directory, run all unit tests.

`cd build && ctest -VV -R "database_*" -j`
