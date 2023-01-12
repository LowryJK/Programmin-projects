/*
#############################################################################
# COMP.CS.110 Programming 2, Autumn 2022                                    #
# Project3: Book contents                                                   #
# File: cli.cpp                                                             #
# Description: Commandline interface for the program.                       #
# Notes: * This is a part of an exercise program                            #
#        * Student's don't touch this file.                                 #
# Check the cli.hh for more info.                                           #
#############################################################################
*/

#include <iostream>
#include <algorithm>

#include "cli.hh"
#include "utils.hh"
#include <fstream>
#include <sstream>

Cli::Cli(std::shared_ptr<Book> db):
    database_(db), canStart_(db != nullptr)
{
}

bool Cli::execPrompt()
{
    if ( not canStart_ )
    {
        std::cout << UNINITIALIZED << std::endl;
        return false;
    }

    // Reading and splitting an input line (command with its parameters)
    std::string cmd;
    std::cout << PROMPT;
    std::getline(std::cin, cmd);
    std::vector<std::string> input = Utils::split(cmd, ' ');
    if ( input.empty() or Utils::isEmpty(cmd) )
    {
        return true;
    }

    // Separating the command and its parameters (arguments)
    cmd = input.front();
    std::vector<std::string> args = popFront(input);

    CommandInfo* func = findCommand(cmd);
    if ( func == nullptr )
    {
        std::cout << UNKNOWN_CMD << std::endl;
        return true;
    }
    if ( func->name_ == "Quit" )
    {
        return false;
    }

    // Help works with zero or more parameters, from which only the first one
    // is taken into account
    if ( func->name_ == "Help" )
    {
        printHelp(args);
        return true;
    }

    // Checking if the command has correct amount of parameters
    if ( func->params_.size() != args.size() )
    {
        std::cout << WRONG_PARAMETERS << std::endl;
        return true;
    }

    // Checking if a numeric parameter is really numeric
    if ( func->numericParamIndex_ >= 0 and
         func->numericParamIndex_ <= static_cast<int>(args.size()) and
         not Utils::isNumeric(args.at(func->numericParamIndex_)) )
    {
        std::cout << NOT_NUMERIC << std::endl;
        return true;
    }

    // Commands can also be read from a file
    if ( func->name_ == "Read")
    {
        return readFromFile(args.at(0));
    }

    // Calling an appropriate member function through the function pointer:
    // (OBJ ->* FUNC_PTR)(PARAMS)
    (database_.get()->*(func->funcPtr_))(args);

    return true;
}

std::vector<std::string> Cli::popFront(std::vector<std::string> &vec)
{
    std::vector<std::string> result;
    for ( unsigned int i = 1; i < vec.size(); ++i )
    {
        if ( not Utils::isEmpty(vec.at(i)) )
        {
            result.push_back(vec.at(i));
        }
    }
    return result;
}

CommandInfo *Cli::findCommand(std::string cmd)
{
    for ( unsigned int i = 0; i < cmd.size(); i++ )
    {
        cmd.at(i) = std::toupper(cmd.at(i));
    }

    for ( auto c = COMMANDS.begin(); c != COMMANDS.end(); c++ )
    {
        for ( auto alias : c->aliases_ )
        {
            if ( alias == cmd )
            {
                return &(*c);
            }
        }
    }
    return nullptr;
}

void Cli::printHelp(const std::vector<std::string>& params)
{
    if ( params.size() != 0 )
    {
        printCommandInfo(*(findCommand(params.at(0))), true);
        return;
    }
    for(unsigned int i = 0; i < COMMANDS.size() - 1; ++i)
    {
        printCommandInfo(COMMANDS.at(i), true);
    }
}

void Cli::printCommandInfo(const CommandInfo& command, bool longer) const
{
    std::cout << command.name_ << " --- " ;
    for ( auto alias : command.aliases_ )
    {
        std::cout << alias << " ";
    }
    if ( longer )
    {
        std::cout << "--- Params: ";
        if ( command.params_.size() == 0 )
        {
            std::cout << "None.";
        }
        else
        {
            for ( auto param : command.params_ )
            {
                std::cout << param << " ";
            }
        }
    }
    std::cout << std::endl;
}

bool Cli::readFromFile(const std::string& fileName)
{
    std::ifstream inputFile(fileName);
    if ( not inputFile )
    {
        std::cout << FILE_READING_ERROR << std::endl;
        return false;
    }

    // Storing std::cin and redirecting the file to be used as input stream
    auto cinbuf = std::cin.rdbuf();
    std::cin.rdbuf(inputFile.rdbuf());

    // Executing commands (from the file)
    while ( execPrompt() );

    // Returning std::cin as the input stream and closing the file
    std::cin.rdbuf(cinbuf);
    inputFile.close();

    std::cout << FILE_READING_OK << fileName << std::endl;
    return true;
}
