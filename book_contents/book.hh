/*
#############################################################################
# COMP.CS.110 Programming 2: Autumn 2022                                    #
# Project3: Book contents                                                   #
# File: book.hh                                                             #
# Description: Class describing book chapter hierarchy                      #
#       Datastructure is populated with Chapter-structs and provides some   #
#       query-functions.                                                    #
# Notes: * This is a part of an exercise program                            #
#        * Student's aren't allowed to alter public interface!              #
#        * All changes to private side are allowed.                         #
#############################################################################
*/

/*
 * Program author:
 * Name: Lauri Koivuniemi
 * */

#ifndef BOOK_HH
#define BOOK_HH

#include <string>
#include <vector>
#include <set>
#include <iostream>
#include <unordered_map>
#include <memory>
#include <algorithm>
#include <map>
#include <utility>

// Named constants to improve readability in other modules
const std::string EMPTY = "";
const int NO_LENGTH = -1;

// Command parameters have been collected into a vector. In this way each
// method implementing a command has the same parameters (amount and type).
// If a command has no parameters, the vector is empty.
using Params = const std::vector<std::string>&;

// Struct for a book chapter
struct Chapter
{
    std::string id_ = EMPTY;
    std::string fullName_ = EMPTY;
    int length_ = 0;
    bool isOpen_ = true;
    Chapter* parentChapter_ = nullptr;
    std::vector<Chapter*> subchapters_;
};

using IdSet = std::set<std::string>;

// Book class, a datastucture for Chapter structs
class Book
{
public:
    // Constructor
    Book();

    // Destructor
    ~Book();

    // Adds a new Chapter to the datastructure.
    void addNewChapter(const std::string& id, const std::string& fullName,
                       int length);

    // Adds a new chapter-subchapter relation.
    void addRelation(const std::string& subchapter,
                     const std::string& parentChapter);

    // Prints all stored chapters: ID, full name and length
    void printIds(Params params) const;

    // Prints the contents page of the book. Chapters are printed in the order,
    // they are in the book, subchapters are indented appropriately.
    // Either '-' or '+' is printed before each chapter, '-' for open chapters
    // and '+' for closed one. If a chapter is open, its subchapters are
    // also listed.
    void printContents(Params params) const;

    // Closes the given chapter and its subchapters.
    void close(Params params) const;

    // Opens the given chapter.
    void open(Params params) const;

    // Opens all chapters.
    void openAll(Params params) const;

    // Prints the amount and names of parent chapters in given distance from
    // the given chapter. Parent chapters are printed in alphabethical order.
    void printParentsN(Params params) const;

    // Prints the amount and names of subchapters in given distance from
    // the given chapter. Subchapters are printed in alphabethical order.
    void printSubchaptersN(Params params) const;

    // Prints the sibling chapters of the given chapter, i.e. the chapters
    // sharing the parent chapter with the given one.
    void printSiblingChapters(Params params) const;

    // Prints the total length of the given chapter, i.e. the sum of lengths
    // of the given chapter and its subchapters.
    void printTotalLength(Params params) const;

    // Prints the longest chapter in the hierarchy of the given chapter.
    void printLongestInHierarchy(Params params) const;

    // Prints the shortest chapter in the hierarchy of the given chapter.
    void printShortestInHierarchy(Params params) const;

    // Prints the direct parent chapter of the given chapter.
    // Students will not implement this method.
    void printParent(Params params) const;

    // Prints the direct subchapters of the given chapter.
    // Students will not implement this method.
    void printSubchapters(Params params) const;

private:
    /* The following functions are meant to make project easier.
     * You can implement them if you want and/or create your own.
     * Anyway it would be a good idea to implement more functions
     * to make things easier and to avoid "copy-paste-coding".
     */

    // key : ID, value : chapter pointer
    using Data = std::vector<std::pair<std::string, Chapter*>>;

    Data chapters_;

    // Returns a pointer for ID.
    Chapter* findChapter(const std::string& id) const;

    // Prints the the data in a container.
    void printGroup(const std::string& id, const std::string& group,
                    const IdSet& container) const;

    // Turns a vector of chapters to a set of IDs.
    // Needed only for printSubchapters.
    IdSet vectorToIdSet(const std::vector<Chapter*>& container) const;

    // Method to print the chapter using recursion
    void printChaptersRecursive(Chapter *ch, int index,
                                const std::string &indentatiton) const;

    // Method to sum the lengths of chapters using recursion
    void countTotalLenghtRecursive(Chapter *ch, int &total) const;

    // Stores lengths of chapters to a vector of pairs
    void storeChapterLengthsRecursive(Chapter *ch, std::vector<std::pair<int, std::string>> &lengths) const;

    // Checks if a chapter is known or not
    bool chapterIsUnknown(const std::string &id) const;

    // Opens a chapter
    void openChapter(Chapter *ch) const;

    // A recursive method used to store count the amount of parent chapters and
    // push the parent chapters' ids to a vector
    void parentChaptersRecursion(Chapter* ch, int &amountOfParentChapters, std::vector<std::string> &parents) const;

    // A recursive method used to store the subchapters of a chapter to a vector
    void subChaptersRecursion(Chapter *ch, std::vector<Chapter *> &subs) const;


};

#endif // BOOK_HH
