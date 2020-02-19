
#include <iostream>
#include "Record.h"
#include <stdlib.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {

	// try to parse the CNF
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	Schema lineitem ("catalog", "lineitem");

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();

	// now open up the text file and start procesing it
        FILE *tableFile = fopen ("/cise/homes/zheyu/git/tpch-dbgen/lineitem.tbl", "r");

        Record temp;
        Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;
	Page page;
	File saveFile;
	saveFile.Open(0, "/cise/homes/zheyu/DBI/Project1/saveFile");
	while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		if(page.Append(&temp) == 0) {
			saveFile.AddPage(&page, saveFile.GetLength());
			page.EmptyItOut();
			page.Append(&temp);
		}
		/*counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		if (comp.Compare (&temp, &literal, &myComparison))
			temp.Print (&mySchema);*/
	}
	saveFile.AddPage(&page, saveFile.GetLength());
	page.EmptyItOut();
	off_t length = saveFile.GetLength();
	off_t count = 0;
	Record result;
	while(count < length - 1) {
		saveFile.GetPage(&page, count);
		while(page.GetFirst(&result) != 0) {
			counter++;
			if (comp.Compare (&result, &literal, &myComparison)) {
				result.Print (&mySchema);
			}
		}
		count++;
	}
	cerr << counter << "\n";
	saveFile.Close();

}


