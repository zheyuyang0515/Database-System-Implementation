#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <string>
typedef enum {heap, sorted, tree} fType;
// stub DBFile header..replace it with your own DBFile.h 

class DBFile {
private:
	Page page;
	File file;
	fType fileType;
	Page readPage;
	off_t counter;
	off_t bufferedCounter;
	char* enumToString(fType type);
	fType stringToEnum(string s);
public:
	DBFile (); 

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
