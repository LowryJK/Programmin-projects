/*
#############################################################################
# COMP.CS.110 Programming 2, Autumn 2022                                    #
# Project3: Book contents                                                   #
# File: cli.hh                                                              #
# Description: Commandline interface for the program.                       #
# Notes: * This is a part of an exercise program                            #
#        * Student's don't touch this file.                                 #
#        * Understanding how the module works isn't necessary to complete   #
#          this exercise.                                                   #
#        * This is also an example how you can use function pointers to     #
#          a class' functions.                                              #
#        * This could be upgraded to have commandhistory with a relative    #
#          ease.                                                            #
#############################################################################
*/

#ifndef CLI_HH
#define CLI_HH

#include "book.hh"
#include <string>
#include <vector>
#include <memory>

/* In order to define a function pointer to a class you have to add
 * class as the visibility range before the pointer declaring * and
 * after that comes the type-name for the pointer.
 * In this case we have named the types according to the amount and
 * type of parameters the function receives.
 */
using MemberFunc = void (Book::*)(const std::vector<std::string>& params) const;

// Struct that describes a user-given command: command name with aliases,
// parameters and a function pointer to the function that implements the
// command and that is going to be called through the pointer.
// The field numericParamIndex_ tells which parameter is a numeric one,
// if the command has no numeric parameters, the field has the value -1.
struct CommandInfo
{
    std::vector<std::string> aliases_;
    std::string name_;
    std::vector<std::string> params_;
    int numericParamIndex_;
    MemberFunc funcPtr_;
};

// Command prompt used in the program
const std::string PROMPT = "Book> ";

// Error strings
const std::string UNINITIALIZED = "Error: Given object hasn't been initialized. Stopping.";
const std::string WRONG_PARAMETERS = "Error: Wrong amount of parameters.";
const std::string NOT_NUMERIC = "Error: Wrong type of parameters.";
const std::string UNKNOWN_CMD = "Error: Unknown commands given.";
const std::string FILE_READING_ERROR = "Error: Can't read given file.";
const std::string FILE_READING_OK = "Input read from file: ";

class Cli
{
public:
    // Constructor
    Cli(std::shared_ptr<Book> db);

    // Runs the interface.
    // Returns false in the case of quit command, or there is no database_,
    // returns true in the case of other commands.
    bool execPrompt();

private:
    // Shared pointer to the whole book, called database_
    std::shared_ptr<Book> database_;
    //Book* database_;

    // Tells if program execution can be started.
    // It cannot be started if there is no database_.
    bool canStart_ = false;

    // Pops the front element from the given vector,
    // and returns the vector without the front element.
    std::vector<std::string> popFront(std::vector<std::string>& vec);

    // Finds the user given command from the COMMANDS vector below,
    // and returns a pointer to the found CommandInfo struct.
    CommandInfo* findCommand(std::string cmd);

    // Generates and prints help, either generic (no parameters) or
    // specific (command name as a parameter).
    void printHelp(const std::vector<std::string>& params);

    // Prints info of a single command.
    void printCommandInfo(const CommandInfo& command, bool longer) const;

    // Returns false if the given file could not be read, true otherwise.
    // Omits informative output, so even if commands are read, they may
    // not have worked.
    bool readFromFile(const std::string& fileName);

    // Constant vector containing different commands that the CLI can recognize
    std::vector<CommandInfo> COMMANDS = {
        {{"IDS","CHAPTERS"},"Chapters",{},-1,&Book::printIds},
        {{"CONTENTS","CONTS"},"Contents",{},-1,&Book::printContents},
        {{"CLOSE","C"},"Close",{"chapter"},-1,&Book::close},
        {{"OPEN","O"},"Open",{"chapter"},-1,&Book::open},
        {{"OPEN_ALL"},"Open all",{},-1,&Book::openAll},
        {{"PARENTS","UP"},"Parents",{"chapter","level"},1,&Book::printParentsN},
        {{"SUBCHAPTERS","DOWN"},"Subchapters",{"chapter","level"},1,&Book::printSubchaptersN},
        {{"SIBLINGS","CO_CHAPTERS"},"Siblings",{"chapter"},-1,&Book::printSiblingChapters},
        {{"LENGTH","TOTAL"},"Length",{"chapter"},-1,&Book::printTotalLength},
        {{"LONGEST","L"},"Longest",{"chapter"},-1,&Book::printLongestInHierarchy},
        {{"SHORTEST","S"},"Shortest",{"chapter"},-1,&Book::printShortestInHierarchy},
        {{"QUIT","Q","EXIT"},"Quit",{},-1,nullptr},
        {{"HELP","H"},"Help",{"command"},-1,nullptr},
        {{"READ","READ_FROM","RF"},"Read",{"file_name"},-1,nullptr},
        {{}, "",{},-1,nullptr}
    };
};

#endif // CLI_HH
