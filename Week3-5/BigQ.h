#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
#include <algorithm>
#include<queue>

using namespace std;
struct runStruct {
	Page *page;
	off_t currentIndex;
	off_t startIndex;
};
struct recordStruct {
	runStruct &runstruct;
	Record &record;
	recordStruct(runStruct &runstruct, Record &record): runstruct(runstruct),record(record) {}
};
struct recordCompStruct {
	OrderMaker &o;
	ComparisonEngine comp;	
	recordCompStruct(OrderMaker &o):o(o){}
	bool operator() (Record *r1, Record *r2) {	
		return comp.Compare(r1, r2, &o) < 0;
	}
};
struct recordHeapCompStruct {
	OrderMaker &o;
	ComparisonEngine comp;	
	recordHeapCompStruct(OrderMaker &o):o(o){}
	bool operator() (recordStruct *r1, recordStruct *r2) {
		return comp.Compare(&r1->record, &r2->record, &o) > 0;
	}
};
struct bigQstruct{
	Pipe &in;
	Pipe &out;
	OrderMaker &sortorder;
	int runlen;
};
class BigQ {
private:	
	static void* worker(void * arg);	
public:
	static int saveRuns(File &file, vector<Record*> &run);
	static int createWorer(bigQstruct *s, pthread_t pId);
	static char* stringToChar(string s);
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
