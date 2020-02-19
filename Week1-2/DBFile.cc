#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <cstring>
using namespace std;
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
}
//Next, there is a function that is used to actually create the file, called Create. 
//The first parameter to this function is a text string that tells you where the binary data is physically to be located – you should store the actual database data using the File class from File.h. 
//If you need to store any meta-data so that you can open it up again in the future 
//(such as the type of the file when you re-open it) 
//as I indicated above, you can store this in an associated text file – 
//just take name and append some extension to it, such as
//.header, and write your meta-data to that file.

//The second parameter to the Create function tells you the type of the file. 
//In DBFile.h, you should define an enumeration called myType with three possible values: heap, sorted, and tree. 
//When the DBFile is created, one of these tree values is passed to Create to tell the file what type it will be. 
//In this assignment, you obviously only have to deal with the heap case. 
//Finally, the last parameter to Create is a dummy parameter that you won’t use for this assignment, 
//but you will use for assignment two. The return value from Create is a 1 on success and a zero on failure.
char* DBFile::enumToString (fType type) {
    switch(type) {
        case heap: return "heap";
        case sorted: return "sorted";
        case tree: return "tree";
    }
}
fType DBFile::stringToEnum (string s) {
    fType type;
    if(s == "heap") {
        type = heap;
    } else if(s == "sorted") {
        type = sorted;
    } else if(s == "tree") {
        type = tree;
    }
    return type;
}
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    char *location = const_cast<char*>(f_path);
    file.Open(0, location);
    //create config file
    char *s = new char[strlen(location) + 7];
    strcpy(s, location);
    strcat(s, ".header");
    FILE *configFile = fopen (s, "w");
    if(configFile == NULL) {
        return 0;
    }
    //write fType to config file
    if(EOF == fputs(enumToString(f_type), configFile)) {
        cerr << "write to file error!!" << endl;
        return 0;
    }
    fclose(configFile);
    return 1;
}

//the Load function bulk loads the DBFile instance from a text file, 
//appending new data to it using the SuckNextRecord function from Record.h. 
//The character string passed to Load is the name of the data file to bulk load.
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *recordFile = fopen (loadpath, "r");
    Record record;
    while (record.SuckNextRecord (&f_schema, recordFile) == 1) {
        Add(record);
    }
    fclose(recordFile);
}
//Next, we have Open. 
//This function assumes that the DBFile already exists and has previously been created and then closed. 
//The one parameter to this function is simply the physical location of the file. 
//If your DBFile needs to know anything else about itself, it should have written this to an auxiliary text file that it will also open at startup. 
//The return value is a 1 on success and a zero on failure.
int DBFile::Open (const char *f_path) {
    char *location = const_cast<char*>(f_path);
    file.Open(1, location);
    //open config file
    char *s = new char[strlen(location) + 7];
    strcpy(s, location);
    strcat(s, ".header");
    FILE *configFile = fopen (s, "r");
    if(configFile == NULL) {
        return 0;
    }
    //read fType
    char type[10]; 
    if(!feof(configFile)) {
        fgets(type, sizeof(type), configFile);
    }
    string str = type;
    fileType = stringToEnum(str);
    fclose(configFile);
    return 1;
}
//Each DBFile instance has a “pointer” to the current record in the file. 
//By default, this pointer is at the first record in the file, 
//but it can move in response to record retrievals. 
//The following function forces the pointer to correspond to the first record in the file
void DBFile::MoveFirst () {
    counter = 0;
    readPage.EmptyItOut();
}
//Close simply closes the file. The return value is a 1 on success and a zero on failure
int DBFile::Close () {
    //save page to file before close
    if(bufferedCounter > 0) {
        file.AddPage(&page, file.GetLength());
        page.EmptyItOut();
        bufferedCounter = 0;
    }
    counter = 0;
    readPage.EmptyItOut();
    file.Close();
    return 1;
}

//In order to add records to the file, the function Add is used. 
//In the case of the unordered heap file that you are implementing in this assignment, 
//this function simply adds the new record to the end of the file
//(Note that this function should actually consume rec, 
//so that after rec has been put into the file, it cannot be used again.)
void DBFile::Add (Record &rec) {
    //if page is full, save to file
    if(page.Append(&rec) == 0) {
        file.AddPage(&page, file.GetLength());
        page.EmptyItOut();
        bufferedCounter = 0;
		page.Append(&rec);
    }
    bufferedCounter++;
}


//The first version of GetNext simply gets the next record from the file and returns it to the user, 
//where “next” is defined to be relative to the current location of the pointer. 
//After the function call returns, the pointer into the file is incremented, 
//so a subsequent call to GetNext won’t return the same record twice. 
//The return value is an integer whose value is zero if and only if there is not a valid record returned from the function call 
//(which will be the case, for example, if the last record in the file has already been returned).
int DBFile::GetNext (Record &fetchme) {
    //save page to avoid dirty
    if(bufferedCounter > 0) {
        file.AddPage(&page, file.GetLength());
        page.EmptyItOut();
        bufferedCounter = 0;
    }
    while(readPage.GetFirst(&fetchme) == 0) {
        off_t length = file.GetLength();
        if(counter == length - 1) {
            return 0;
        }
        file.GetPage(&readPage, counter);
        counter++;
    }
    return 1;
}

//The next version of GetNext also accepts a selection predicate (this is a conjunctive normal form expression). 
//It returns the next record in the file that is accepted by the selection predicate. 
//The literal record is used to check the selection predicate, and is created when the parse tree for the CNF is processed.
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    //save page to avoid dirty
    if(bufferedCounter > 0) {
        file.AddPage(&page, file.GetLength());
        page.EmptyItOut();
        bufferedCounter = 0;
    }
    ComparisonEngine comp;
    while(GetNext(fetchme) != 0) {
        if(comp.Compare (&fetchme, &literal, &cnf)) {
            return 1;
        }
    }
    return 0;
}
