/*
#############################################################################
# COMP.CS.110 Programming 2, Autumn 2022                                    #
# Project3: Book contents                                                   #
# Program description: Queries user for CSV file and allows simple searches #
#       based on the data.                                                  #
# File: main.cpp                                                            #
# Description: Main-module performs the followin operations:                #
#       * Queries for input file                                            #
#       * Parses the CSV data                                               #
#       * Passes the parsed data to Book class                              #
#       * Launches Cli module                                               #
# Notes: * This is an exercise program.                                     #
#        * Student's don't touch this file.                                 #
#############################################################################
*/

#include "book.hh"
#include "cli.hh"
#include "utils.hh"
#include <iostream>
#include <string>

int main()
{
    std::string fileName;
    std::shared_ptr<Book> database(new Book);

    // File query and opening
    std::cout << "Input file: ";
    std::getline(std::cin, fileName);
    if ( not Utils::readInputFile(fileName, database) )
    {
        return EXIT_FAILURE;
    }

    // Constructing the commandline interpreter with the datastructure
    // formed based on the given input file
    Cli commandline(database);

    // Empty loop that runs the CLI.
    // CLI returns false only on quit command.
    while ( commandline.execPrompt() );

    return EXIT_SUCCESS;
}
