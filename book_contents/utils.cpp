/*
#############################################################################
# COMP.CS.110 Programming 2, Autumn 2022                                    #
# Project3: Book contents                                                   #
# File: utils.cpp                                                           #
# Description: Module for different utilities needed in the program.        #
# Notes: * This is a part of an exercise program                            #
#        * Student's don't touch this file.                                 #
# Check the utils.hh for more info.                                         #
#############################################################################
*/

#include "utils.hh"
#include <vector>
#include <fstream>

namespace Utils
{

namespace // private part
{

// Struct to group-up boss-subordinate relations.
struct Relation
{
    std::string subChapter_;
    std::string parentChapter_;
};

// Constants to make CSV parsing more readable.
const char CSV_DELIMITER = ';';
enum CsvParts {CSV_ID, CSV_FULL, CSV_LENGTH, CSV_PARENT, CSV_VALUES};

// Parses the given line to a new 'chapter - parent chapter' relation.
// Adds the new relation in the relations vector and returns the Chapter struct
// that was created, with the following indications:
// * id_     - EMPTY if wrong amount of params
// * length_ - NO_LENGTH if wrong amount of params or stoi conversion failed
Chapter parseInputLine(const std::string& line,
                       std::vector<Relation> &relations)
{
    Chapter newChapter;
    Relation newRelation;

    // Checking that there is correct amount of fields in the line
    std::vector<std::string> parsedData = split(line, CSV_DELIMITER);
    if ( parsedData.size() == CSV_VALUES )
    {
        newChapter.id_ = parsedData.at(CSV_ID);
        newChapter.fullName_ = parsedData.at(CSV_FULL);

        // Exception safety.
        // The NO_LENGTH assignment isn't necessary, but makes things clearer.
        try
        {
            newChapter.length_ = std::stoi(parsedData.at(CSV_LENGTH));
        }
        catch(std::invalid_argument&)
        {
            std::cout << "Caught invalid_argument exception." << std::endl;
            newChapter.length_ = NO_LENGTH;
        }
        catch(std::out_of_range&)
        {
            std::cout << "Caught out_of_range exception." << std::endl;
            newChapter.length_ = NO_LENGTH;
        }
        newRelation.subChapter_ = parsedData.at(CSV_ID);
        newRelation.parentChapter_ = parsedData.at(CSV_PARENT);
        relations.push_back(newRelation);
    }

    return newChapter;
}

// Reads the given datafile and populates the database with it.
// Returns true if there are no errors in the file, otherwise returns false.
bool populateDatabase(std::ifstream& datafile, std::shared_ptr<Book> database)
{
    std::string line = "";
    std::vector<Relation> relations;
    int lineNumber = 0;

    while ( std::getline(datafile, line) )
    {
        // Linenumbering for error-printing
        ++lineNumber;

        // Skipping empty and commented lines
        if ( line.empty() or line.at(0) == '#' )
        {
            continue;
        }

        // Parsing the line to a new chapter and relation
        Chapter newChapter = parseInputLine(line, relations);
        if ( isEmpty(newChapter.id_) or isEmpty(newChapter.fullName_) or
             newChapter.length_ == NO_LENGTH )
        {
            std::cout << "Error in datafile, line " << lineNumber << std::endl;
            return false;
        }

        // Adding the new chapter to the database
        database->addNewChapter(newChapter.id_,
                                newChapter.fullName_,
                                newChapter.length_);
    }

    // After chapters have been created, adding the 'chapter - parent chapter'
    // relations.
    for ( auto iter = relations.begin(); iter != relations.end(); ++iter )
    {
        database->addRelation(iter->subChapter_, iter->parentChapter_);
    }
    return true;
}

} // end of nameless namespace

std::vector<std::string> split(const std::string& str, char delim)
{
    std::vector<std::string> result = {""};
    bool insideQuotation = false;
    for ( char currentChar : str )
    {
        if ( currentChar == '"' )
        {
            insideQuotation = not insideQuotation;
        }
        else if ( currentChar == delim and not insideQuotation)
        {
            result.push_back("");
        }
        else
        {
            result.back().push_back(currentChar);
        }
    }
    return result;
}

bool isNumeric(std::string str)
{
    for ( char ch : str)
    {
        if ( not isdigit(ch) )
        {
            return false;
        }
    }
    return true;
}

bool isEmpty(const std::string& str)
{
    for ( char ch : str )
    {
        if ( ch != ' ' )
        {
            return false;
        }
    }
    return true;
}

bool readInputFile(const std::string& fileName, std::shared_ptr<Book> database)
{
    std::ifstream input(fileName);
    if ( not input )
    {
        std::cout << "Could not open file: " << fileName << std::endl;
        return false;
    }
    return populateDatabase(input, database);
}

} // end of namespace Utils
