/*
#############################################################################
# COMP.CS.110 Programming 2, Autumn 2022                                    #
# Project3: Book contents                                                   #
# Program description: Queries user for CSV file and allows simple searches #
#       based on the data.                                                  #
# File: book.cpp                                                            #
# Description: book.cpp file has:
  - Methods that allows the user to run commands to the data given (book)
  - Comments on the methods and how they work
  - Constructor and deconstructor of the class Book                         #
# Notes:                                                                    #
#############################################################################
*/

/*
 * Program author:
 * Name: Lauri Koivuniemi
 * */

#include "book.hh"
//Constructor
Book::Book():
    chapters_({})
{

}
//Deconstructor
Book::~Book()
{
    for(auto pair : chapters_) {
        delete pair.second;
    }
}

// A method to add a new chapter to using struct Chapter
// params : string id = chapter's id,
//          string fullName = chapter's full name,
//          int length = length of the chapter
//
void Book::addNewChapter(const std::string &id, const std::string &fullName, int length)
{
    // If id already exists, prints error
    if (!chapterIsUnknown(id)) {
        std::cout << "Error: Already exists." << std::endl;
    }
    // Create a new chapter pointer with given parameters.
    // By default chapter is open, has no parent or subchapters
    Chapter *newCh = new Chapter{id, fullName, length, true, nullptr, {}};

    // Add the new chapter's id and the new chapter to the
    // vector of pairs a.k.a. chapters_
    std::string newId = newCh->id_;
    chapters_.push_back({newId, newCh});

}

// A method to add relations (subchapter and parentchapter) to a chapter
// params: string subchapter, name of the subchapter
//         string parentchapter, name of the parentchapter
void Book::addRelation(const std::string &subchapter, const std::string &parentChapter)
{
        // Pointers for parent and child chapters.
        // Get them with findChapter method.
        Chapter *parent = findChapter(parentChapter),
                 *child = findChapter(subchapter);

        // If the child pointer is a nullptr, print error
        if (child == nullptr) {
            std::cout << "Error" << std::endl;

        }
        // If the parent pointer is a nullptr, return
        if (parent == nullptr) {
            return;
        }

        // Add the child chapter to the parents subchapter
        parent->subchapters_.push_back(child);

        // Add the parent chapter as the child's parent chapter
        child->parentChapter_ = parent;

}

// A method to print all of the chapters Ids and fullNames
void Book::printIds(Params ) const
{
    // Amount of chapters put in a variable called size
    size_t size = chapters_.size();

    // Print the amount of chapters the book has
    std::cout << "Book has " << size << " chapters:" << std::endl;

    // Map of strings including the Ids and fullNames
    std::map<std::string, std::string> idsAndfullNames;

    // For every pair in chapters_ add the fullName of the chapter
    // and the chapter's Id to the map idsAndfullNames
    for (auto pair: chapters_) {
        idsAndfullNames.insert({pair.second->fullName_, pair.first});

    }

    // For every pair in idsAndFullNames print the fullName and id.
    for (auto pair : idsAndfullNames) {
        std::cout << pair.first << " --- " << pair.second << std::endl;
    }

}

// A method to print all the contents of a book
void Book::printContents(Params ) const
{
    // Vector that contains the parent chapters
    std::vector<Chapter *> headChapters = {};

        // For loop that pushes the parent chapters
        // to the vector headChapters
        for(auto pair: chapters_) {
            if (pair.second->parentChapter_ == nullptr) {
                headChapters.push_back(pair.second);
        }
    }
    // Index needed for the recursion, start from 1
    int index = 1;

    // For loop that loops through the headChapters
    for (auto *ptr : headChapters) {
        // Implement the printChaptersRecursive method
        printChaptersRecursive(ptr, index, " ");

        // Up the index by one
        index++;
    }
}

// A method for closing chapters
void Book::close(Params params) const
{
        // Id of the chapter is gotten from the vector params
        std::string id = params.at(0);

        // If the id is unknown print error message
        if (chapterIsUnknown(id)) {
            std::cout << "Error: Not found: " << id << std::endl;
            return;
        }

        // Chapter ptr gotten with findChapter
        Chapter *ch = findChapter(params.at(0));

        // close the chapter if it is open
        ch->isOpen_ = false;

        // Also close all the subchapters of the chapter
        // using recursion
        for (auto sub : ch->subchapters_) {
            close({sub->id_});
        }
}

// A method to open a chapter
void Book::open(Params params) const
{
    // Id of the chapter is gotten from the vector params
    std::string id = params.at(0);

    // If the id is unknown print error message
    if (chapterIsUnknown(id)) {
        std::cout << "Error: Not found: " << id << std::endl;
        return;
    }
        // Chapter ptr gotten with findChapter
        Chapter *ch = findChapter(params.at(0));

        // Open the chapter using openChapter method
        openChapter(ch);

}

// A method to open all chapters
void Book::openAll(Params ) const
{
    // For loop that goes through all the chapters
    // and opens them.
    for (auto pair : chapters_) {
        openChapter(pair.second);
    }
}

// A method to print the amount and names of parent chapters in given distance from
// the given chapter. Parent chapters are printed in alphabethical order.
void Book::printParentsN(Params params) const
{
    // Id of the chapter is gotten from the vector params.
    // Also the number "N" (distance of the given chapter)
    // is gotten from the vector params.
    std::string id = params.at(0);
    std::string N = params.at(1);

    // If the id is unknown print error message
    if (chapterIsUnknown(id)) {
        std::cout << "Error: Not found: " << id << std::endl;
        return;
    }

    // Change the N from string to int
    int intN = std::stoi(N);

    // If N is less than 1, print error
    if (intN < 1) {
        std::cout << "Error. Level can't be less than 1." << std::endl;
        return;
    }

    // Initialize a chapter with the given id
    Chapter *ch = findChapter(id);

    // If chapter has no parent chapter, print message
    if (ch->parentChapter_ == nullptr) {
        std::cout << ch->id_ << " has no parent chapters." << std::endl;
        return;
    }

    // Initialize amount of parents as 0 in beginning
    int amountOfParentChapters = 0;

    // A vector storing all the parents ids.
    std::vector<std::string> parents = {};

    // A vector storing parents from the given N level
    std::vector<std::string> parentsOfGivenN = {};

    // Recursive method to push the parent chapters to the vector parents
    parentChaptersRecursion(ch, amountOfParentChapters, parents);

    // If there's more parent chapters than the user input as N,
    // print the amount of parent chapters as N. (distance of given chapter)
    if (amountOfParentChapters > intN) {
        std::cout << ch->id_ << " has " << intN
                  << " parent chapters:" << std::endl;

        // Push the parent chapters from the "parents" vector
        // to "parentsOfGivenN" vector"
        for (int i = 0; i < intN; ++i) {
            parentsOfGivenN.push_back(parents.at(i));
        }

        // Sort the "parentsOfGivenN" vector
        std::sort(parentsOfGivenN.begin(), parentsOfGivenN.end());

        // Print the parent chapters in the "parentsOfGivenN"
        // vector in alphabetical order
        for ( auto parent : parentsOfGivenN) {
            std::cout << parent << std::endl;
        }
        return;
    }

    // Here the N is equal or higher than the amountOfParentChapters
    // so the program will print the amountOfParentChapters
    else {
        std::cout << ch->id_ << " has "
                  << amountOfParentChapters
                  << " parent chapters:" << std::endl;

        // Print the parent chapters in the vector
        // "parents" in alphabetical order
        for (auto parent : parents) {
            std::sort(parents.begin(), parents.end());
            std::cout << parent << std::endl;
        }
        return;
    }
}

// A method to print the amount and names of subchapters in given distance from
// the given chapter. Subchapters are printed in alphabethical order.
void Book::printSubchaptersN(Params params) const
{

    // Id of the chapter is gotten from the vector params.
    // Also the number "N" (given distance) is gotten from the vector params.
    std::string id = params.at(0);
    std::string N = params.at(1);

    // If the id is unknown print error message
    if (chapterIsUnknown(id)) {
        std::cout << "Error: Not found: " << id << std::endl;
        return;
    }

    // Change the N from string to int
    int intN = std::stoi(N);

    // If N is less than 1, print error
    if (intN < 1) {
        std::cout << "Error. Level can't be less than 1." << std::endl;
        return;
    }

    // Initalize the chapter with given id
    Chapter *ch = findChapter(id);

    // If chapter has no subchapters print message
    if (ch->subchapters_.empty()) {
        std::cout << ch->id_ << " has no subchapters." << std::endl;
    }


    // A vector that stores the subchapters
    std::vector<Chapter*> subs = {};

    // A vector of pairs that stores the level of the subchapter as the first (int)
    // and it's id as second (string)
    std::vector<std::pair<int, std::string>> NandIdPairs = {};

    // A vector that stores the ids of the subchapters
    std::vector<std::string> ids = {};

    // Calls the recursive method for the subchapters. Stores them
    // to the "subs" vector
    subChaptersRecursion(ch, subs);

    // For a chapter in the vector "subs" call the
    // "parentChaptersRecursion" method. From the
    // new vector "parents" push back the level of subchapter
    // and the subchapter's id to the vector of pairs "NandIdPairs"
    for (auto chapter : subs) {
        int levelOfSubchapter = 0;
        std::vector<std::string> parents = {};
        parentChaptersRecursion(chapter, levelOfSubchapter, parents);
        NandIdPairs.push_back({levelOfSubchapter, chapter->id_});
    }

    // Sort the "NandIdPairs" vector
    std::sort(NandIdPairs.begin(), NandIdPairs.end());

    // For every pair in the "NandIdPairs" vector,
    // if the level of the subchapter is equal or smaller
    // than the given "intN" push the subchapter's id to the
    // "ids" vector
    for (auto pair : NandIdPairs) {
        if (pair.first <= intN) {
            ids.push_back(pair.second);
        }
    }

    // Sort the vector "ids"
    std::sort(ids.begin(), ids.end());

    // If chapter has subchapters, prints message telling
    // how many subchapters a chapter has based on the given level
    // "intN"
    if (!ch->subchapters_.empty()) {
    std::cout << ch->id_ << " has " << ids.size() << " subchapters:" << std::endl;

    // Print the ids of the subchapters
    for (auto id : ids)  {
        std::cout << id << std::endl;
    }
  }

}

// A method to print the sibling chapters of the given chapter, i.e. the chapters
// sharing the parent chapter with the given one.
void Book::printSiblingChapters(Params params) const
{
    // Id of the chapter is gotten from the vector params
    std::string id = params.at(0);

    // If the id is unknown print error message
    if (chapterIsUnknown(id)) {
        std::cout << "Error: Not found: " << id << std::endl;
        return;
    }
        // Chapter ptr gotten with findChapter
        Chapter *ch = findChapter(params.at(0));

        // If the chapter has no parentchapter, it's a
        // main chapter so it doesnt have sibling chapters.
        // Prints info that chapter doesnt have any sibling chapters.
        if (ch->parentChapter_ == nullptr) {
            std::cout << ch->id_ << " has no sibling chapters." << std::endl;
        }

        // Chapter has a parent chapter; isn't a main chapter
        else {
            // Get the parent chapter of the chapter
            Chapter* parentCh = ch->parentChapter_;

            // Get the subchapters of the parent chapter; siblings
            std::vector<Chapter*> subs = parentCh->subchapters_;

            // Erase the chapter itself from it's siblings. (You are not your own sibling)
            subs.erase(std::remove(subs.begin(), subs.end(), ch), subs.end());

            // Variable for size of the siblings
            size_t amountOfSiblings = subs.size();

            // Print info of how many siblings the chapter has and also the siblings
            std::cout << ch->id_ << " has " <<amountOfSiblings <<
                         " sibling chapters:" << std::endl;
            for (auto element : subs) {
                std::cout << element->id_ << std::endl;
            }

        }

}

// A method thats prints the total lenght of a chapter and it's subchapter
// combined
void Book::printTotalLength(Params params) const
{
    // Id of the chapter is gotten from the vector params
    std::string id = params.at(0);

    // If the id is unknown print error message
    if (chapterIsUnknown(id)) {
        std::cout << "Error: Not found: " << id << std::endl;
        return;
    }

    // Initalize the total length as 0
    int totalLength = 0;

    // Chapter ptr gotten with findChapter
    Chapter *ch = findChapter(params.at(0));

    // Use the printTotalLengthRecursive method to get the total length
    countTotalLenghtRecursive(ch, totalLength);

    // Print the total lenght of the chapter
    std::cout << "Total length of "
        << ch->id_ << " is "<< totalLength + ch->length_
        << "." << std::endl;

}

// A method that prints the longest chapter in a given hierarchy
void Book::printLongestInHierarchy(Params params) const
{
    // Id of the chapter is gotten from the vector params
    std::string id = params.at(0);

    // If the id is unknown print error message
    if (chapterIsUnknown(id)) {
        std::cout << "Error: Not found: " << id << std::endl;
        return;
    }

    // A vector that stores all the lengths of the hierarchy's chapters
    std::vector<std::pair<int, std::string>> lengthsOfChapters = {};

    // Chapter ptr gotten with findChapter
    Chapter *ch = findChapter(params.at(0));

    // Use the printLongestRecursive to store the chapter lengths
    // and their id's to the vector lengthOfChapters
    storeChapterLengthsRecursive(ch, lengthsOfChapters);

    // Also push the chapter itself to the vector
    lengthsOfChapters.push_back({ch->length_, ch->id_});

    // Size of the amount of chapters in the hierarchy
    size_t amountOfChapters = lengthsOfChapters.size();

    if (amountOfChapters > 1) {

        // Sort the vector including the lengths
        std::sort(lengthsOfChapters.begin(), lengthsOfChapters.end());

        // Checks that the longest chapter isn't the chapter on top of
        // the hierarchy. Prints the lenght of the longest chapter
        // and it's id and also the chapter in which hierarchy it is
        if (lengthsOfChapters.at(amountOfChapters-1).second != ch->id_) {
                std::cout << "With the length of " << lengthsOfChapters.at(amountOfChapters-1).first
                  << ", " << lengthsOfChapters.at(amountOfChapters-1).second
                  << " is the longest chapter in " << ch->id_
                  << "'s hierarchy." << std::endl;
                }

        // If the longest chapter is on top of the hierarchy print
        // a little different message but it has the same point
        if (lengthsOfChapters.at(amountOfChapters-1).second == ch->id_){
            std::cout << "With the length of " << lengthsOfChapters.at(amountOfChapters-1).first
              << ", " << lengthsOfChapters.at(amountOfChapters-1).second
              << " is the longest chapter in their hierarchy." << std::endl;
        }
    }

    // If there is only one chapter in the hierarchy it has to
    // also be the longest.
    if (amountOfChapters == 1) {
        std::cout << "With the length of " << ch->length_
                  << ", " << ch->id_
                  << " is the longest chapter in their hierarchy." << std::endl;
    }
}

// A method to print the shortest chapter in the hierarchy of the given chapter.
void Book::printShortestInHierarchy(Params params) const
{
    // Id of the chapter is gotten from the vector params
    std::string id = params.at(0);

    // If the id is unknown print error message
    if (chapterIsUnknown(id)) {
        std::cout << "Error: Not found: " << id << std::endl;
        return;
    }

    // A vector that stores all the lengths of the hierarchy's chapters
    std::vector<std::pair<int, std::string>> lengthsOfChapters = {};

    // Chapter ptr gotten with findChapter
    Chapter *ch = findChapter(params.at(0));

    // Use the same recursion method as the printLongestInHierarchy method does
    storeChapterLengthsRecursive(ch, lengthsOfChapters);

    // Also push the chapter itself to the vector)
    lengthsOfChapters.push_back({ch->length_, ch->id_});

    // Size of the amount of chapters in the hierarchy
    size_t amountOfChapters = lengthsOfChapters.size();

    // The next functionalities are exactly the same as in the printLongestHierarchy
    // method but now we just use the vectors (lengthsOfChapters) first element and
    // not the last.
    if (amountOfChapters > 1) {

        std::sort(lengthsOfChapters.begin(), lengthsOfChapters.end());

        if (lengthsOfChapters.at(0).second != ch->id_) {
                std::cout << "With the length of " << lengthsOfChapters.at(0).first
                  << ", " << lengthsOfChapters.at(0).second
                  << " is the shortest chapter in " << ch->id_
                  << "'s hierarchy." << std::endl;
                }
        if (lengthsOfChapters.at(0).second == ch->id_){
            std::cout << "With the length of " << lengthsOfChapters.at(0).first
              << ", " << lengthsOfChapters.at(0).second
              << " is the shortest chapter in their hierarchy." << std::endl;
        }

    }
    if (amountOfChapters == 1) {
        std::cout << "With the length of " << ch->length_
                  << ", " << ch->id_
                  << " is the shortest chapter in their hierarchy." << std::endl;
    }
}

void Book::printParent(Params ) const
{
    // Students don't implement this
}

void Book::printSubchapters(Params ) const
{
    // Student's dont implement this
}

// A method that returns an empty ptr if the id is not
// found in the chapters_ vector and the chapter's pointer
// if the id is found in the chapters_ vector.
Chapter *Book::findChapter(const std::string &id) const
{
    // Initialize the ptr as nullptr
    Chapter *ptr = nullptr;

    // Loop through the chapters_ vector
    // to find the id and to return the chapter
    for (auto pair : chapters_) {
        if (pair.first == id) {
            return pair.second;
        }
    }

    // If id is not found return the nullptr
    return ptr;
}

// This method is not used in the program but I couldn't delete it
// for the program to run
void Book::printGroup(const std::string &id, const std::string &group, const IdSet &container) const
{
    std::string a = id;
    std::string b = group;
    IdSet c = container;
}

// Turns a vector of chapters to a set of IDs.
IdSet Book::vectorToIdSet(const std::vector<Chapter *> &container) const
{
    IdSet ids = {};
    for(Chapter *ptr : container) {
        ids.insert(ptr->id_);
    }
    return ids;
}

// A recursive method that prints the chapter in their
// correct hierarchy (intendation)
// params : Chapter *ch, a Chapter struct ptr
//          int index, index of the chapter
//          string intendation, intendation between
//          parent chapter and subchapter
void Book::printChaptersRecursive(Chapter *ch, int index,
                                  const std::string &intendation) const
{
    // If the chapter is open, sign is "-", else "+"
    char sign = ch->isOpen_ ? '-' : '+';

    // Print the chapters fullName and it's length
    // and also the proper sign, intendation and index
    std::cout << sign << intendation << index
              << ". " << ch->fullName_ << " ( "
              << ch->length_ << " )" << std::endl;

    // If chapter is closed or it has no subchapters
    // return.
    if (!ch->isOpen_ || ch->subchapters_.empty()) {
        return;
    }
    // Initialize the index as 1
    index = 1;

    // For loop that goes through the subchapters of
    // the chapter. Ups the index.
    for(Chapter *subch : ch->subchapters_) {
        printChaptersRecursive(subch, index, intendation + "  ");
        ++index;
    }
}

// A method for recursion that counts the total length
// of chapter's subchapters combined
void Book::countTotalLenghtRecursive(Chapter *ch, int &total) const
{
    // If chapter has subchapters use a for loop
    // that sums the length of the subchapters
    if (!ch->subchapters_.empty()) {
        for (Chapter *sub : ch->subchapters_) {
            total += sub->length_;
            // Recursion with the subchapters
            countTotalLenghtRecursive(sub, total);
    }


    }
}

// A method for recursion that stores the lengths
// of the chapters to a vector of pairs
void Book::storeChapterLengthsRecursive(Chapter *ch, std::vector<std::pair<int, std::string> > &lengths) const
{
    // If chapter has subchapters use a for loop
    // that pushes the lengths and ids of the subchapters
    // to the vector of pairs
    if (!ch->subchapters_.empty()) {
        for (Chapter *sub : ch->subchapters_) {
            lengths.push_back({sub->length_, sub->id_});
            // Recursion with the subchapters
            storeChapterLengthsRecursive(sub, lengths);
    }
    }
}

// A boolean method that returns true if the id isn't
// int the chapters_ vector (doesnt exist), returns false if
// id already exists
bool Book::chapterIsUnknown(const std::string &id) const
{
    // A vector of strings for the ids
    std::vector<std::string> ids = {};

    // For loop that pushes all the ids of
    // the existing chapters to the "ids" vector
    for (auto pair : chapters_) {
        ids.push_back(pair.first);
    }

    // Id exists
    if (std::find(ids.begin(), ids.end(), id) == ids.end()) {
        return true;
    }
    // Id doesn't exist
    return false;
}

// A method that opens a chapter
void Book::openChapter(Chapter *ch) const
{
    // Opens a chapter by turning the boolean value
    // of the isOpen_ method to true (if it isn't already open)
    ch->isOpen_ = true;

    // For a subchapter in chapter's subchapters check
    // if the subchapter has subchapters. If it doesn't
    // have subchapters also open that chapter.
    for (auto sub : ch->subchapters_) {
        if (sub->subchapters_.empty()) {
            sub->isOpen_ = true;
        }
    }
}

// A recursive method used to store count the amount of parent chapters and
// push the parent chapters' ids to a vector
void Book::parentChaptersRecursion(Chapter *ch, int &amountOfParentChapters, std::vector<std::string> &parents) const
{
    Chapter *parentChapter = ch->parentChapter_;
    ++amountOfParentChapters;
    parents.push_back({parentChapter->id_});

    // If chapter has a parent chapter continue the recursion
    if (parentChapter->parentChapter_ != nullptr) {
        parentChaptersRecursion(parentChapter, amountOfParentChapters, parents);
    }

}

// A recursive method used to store the subchapters of a chapter to a vector
void Book::subChaptersRecursion(Chapter *ch, std::vector<Chapter *> &subs) const
{  
    for (auto subch : ch->subchapters_) {
        subs.push_back({subch});
        subChaptersRecursion(subch, subs);
    }

}





