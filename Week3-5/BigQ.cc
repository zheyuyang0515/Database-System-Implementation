#include "BigQ.h"

int BigQ::createWorer(bigQstruct *s, pthread_t pId) {
	if(s == NULL) {
		return -1;
	}
	return pthread_create(&pId, NULL, worker, s);
}
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	//define a struct as the parameter which need to be passed to the thread
	bigQstruct *s = new bigQstruct {in, out, sortorder, runlen};
	//create a worker thread
	pthread_t pId;
	int result = createWorer(s, pId);
	//thread create result check
	if(result != 0) {
		cerr << "Create thread for BigQ error." << endl;
		exit(1);
	}
    // finally shut down the out pipe
	//out.ShutDown ();
}
int BigQ::saveRuns(File &file, vector<Record*> &run) {
	//buffer
	Page bufferPage;
	//if(counterbuffer != 0) means the bufferPage is not empty
	int countbuffer;
	for (Record *r : run) {
		if(bufferPage.Append(r) == 0) {
			file.AddPage(&bufferPage, file.GetLength() - 1);
			bufferPage.EmptyItOut();
			bufferPage.Append(r);
			countbuffer = 0;
		}
		countbuffer++;
	}
	if(countbuffer > 0) {
		file.AddPage(&bufferPage, file.GetLength() - 1);
	}
	return file.GetLength();
}
char* BigQ::stringToChar(string s) {
	return const_cast<char*>(s.c_str());
}
void* BigQ::worker(void * arg) {
	//define a file instance to store the temporary data which needs to be sorted
	File file;
	//open the file	
	file.Open(0, stringToChar("BigQtemp"));
	//struct
	bigQstruct *s = (bigQstruct *)arg;
	//to receive record from input pipe
	Record record;
	//store record into a run in order to sort
	vector<Record*> run;
	//current number of pages;
	int currentLen;
	//page buffer
	Page page;
	while(s->in.Remove(&record) == 1) {	
		//deep copy a record and push it into vector
		Record *currentRecord = new Record();	
		currentRecord->Copy(&record);
		//if the page is full
		if(page.Append(&record) == 0) {
			currentLen++;			
			//if the number of pages is equals to runlength
			if(currentLen == s->runlen) {
				currentLen = 0;
				//sort the run
				sort(run.begin(), run.end(), recordCompStruct(s->sortorder));
				saveRuns(file, run);
				//clear the run
				run.clear();
			}
			page.EmptyItOut();
			page.Append(&record);
		}
		run.push_back(currentRecord);
	}
	//if pages is not empty and is less than runlen, then sort it
	if(run.size() > 0) {
		currentLen = 0;
		//sort the run
		sort(run.begin(), run.end(), recordCompStruct(s->sortorder));
		saveRuns(file, run);
		//clear the run
		run.clear();
		page.EmptyItOut();
	}
	//track the current pages of all runs
	vector<runStruct*> curPages;
	off_t whichPage = -1;
	//add first page of each run into vector
	while(whichPage < file.GetLength() - 1) {
		Page *p = new Page();
		file.GetPage(p, whichPage);
		runStruct *r = new runStruct{p, (off_t)(whichPage + 1), whichPage};
		curPages.push_back(r);
		whichPage += (off_t)s->runlen;
	}
	//define a in-memory priority queue to sort heads of runs
	priority_queue<recordStruct*, vector<recordStruct*>, recordHeapCompStruct> heap(s->sortorder);
	//init heap
	for(runStruct *r : curPages) {
		Record *re = new Record;
		r->page->GetFirst(re);
		recordStruct *recordstruct = new recordStruct{*r, *re};
		heap.push(recordstruct);
	}
	//Schema schema ("catalog", "customer");
	//continuely pop the top of the heap and push next record of the top element until the heap is empty.
	while(!heap.empty()) {
		recordStruct* rs = heap.top();
		heap.pop();
		Record *record = new Record();
		record->Consume(&rs->record);
		s->out.Insert(record);
		Record *r = new Record();
		//move to the next page
		if(rs->runstruct.page->GetFirst(r) == 0) {
			//reach the end of a run
			if(rs->runstruct.currentIndex - rs->runstruct.startIndex == s->runlen || rs->runstruct.currentIndex == file.GetLength() - 1) {
				continue;
			}
			file.GetPage(rs->runstruct.page, rs->runstruct.currentIndex);
			rs->runstruct.currentIndex++;
			rs->runstruct.page->GetFirst(r);
		}
		//modify the record in recordStruct
		rs->record.Consume(r);
		//push it back to the heap
		heap.push(rs);
	}
	//release memory
	s->out.ShutDown();
	delete s;
	file.Close();	
}

BigQ::~BigQ () {
}
