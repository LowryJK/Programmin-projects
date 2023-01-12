/*
#############################################################################
# COMP.CS.110 Programming 2, Autumn 2022                                    #
# Project3: Book contents                                                   #
# File: utils.hh                                                            #
# Description: Module for different utilities needed in the program.        #
# Notes: * This is a part of an exercise program                            #
#        * Student's don't touch this file.                                 #
#        * Understanding how the module works isn't necessary to complete   #
#          this exercise.                                                   #
#        * This is also an example on a module that is not a class.         #
#############################################################################
*/

#ifndef UTILS_HH
#define UTILS_HH

#include "book.hh"
#include <string>

namespace Utils{

// Casual split function, if delim char is between "'s, ignores it.
std::vector<std::string> split(const std::string& str, char delim);

// Returns true if the given string consists only of digits,
// otherwise returns false.
bool isNumeric(std::string str);

// Returns true if the given string consists only of empty spaces (' '),
// otherwise returns false.
bool isEmpty(const std::string& str);

// Returns true if the file of the given name can be opened and if all the
// lines in the file follow the required format, otherwise returns false.
// In the succesful (true) case, passes shared pointer to the populated
// database in the second parameter.

bool readInputFile(const std::string& fileName, std::shared_ptr<Book> database);

}

#endif // UTILS_HH
