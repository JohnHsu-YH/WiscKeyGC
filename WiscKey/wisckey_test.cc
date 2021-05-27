#include "lab2_common.h"
#include <fstream>
#include <algorithm> 
#include <vector>      
#include <ctime>       
#include <cstdlib>    

// Author: Abhishek Sharma
// Program: WiscKey Key Value Store

typedef struct WiscKey {
  string dir;
  DB * leveldb;
  FILE * logfile;
} WK;
FILE *logfile;
DB * db;
static std::string headkey = "head";
static bool wisckey_get(WK * wk, string &key, string &value)
{	
	cout << "\n\t\tGet Function\n\n";
	cout << "Key Received: " << key << endl;

	string offsetinfo;
        bool found = leveldb_get(wk->leveldb, key, offsetinfo);
        if (found) {
       		cout << "Offset and Length: " << offsetinfo << endl;//call by address直接修改成可以用來取值的offset以及value size
        }
        else {
       	        cout << "Record:Not Found" << endl;
		return false;
        }
        //這裡做拆解offset資訊
	std::string value_offset;
	std::string value_length;
	std::string s = offsetinfo;
	std::string delimiter = "&&";
	size_t pos = 0;
	std::string token;
	while ((pos = s.find(delimiter)) != std::string::npos) {
    		token = s.substr(0, pos);
		value_offset = token;
    		s.erase(0, pos + delimiter.length());
	}
	value_length = s;
	//這裡做拆解offset資訊
	cout << "Value Offset: " << value_offset << endl;
	cout << "Value Length: " << value_length << endl;

  	std::string::size_type sz;
  	long offset = std::stol (value_offset,&sz);
	long length = std::stol (value_length,&sz);
	//rewind(wk->logfile);
	//cout << offset << length << endl;
	std::string value_record;
	//cout << ftell(wk->logread) << endl;
	fseek(wk->logfile,offset,SEEK_SET);
	//cout << ftell(wk->logfile) << endl;
	//rewind(wk->logfile);
	//cout << ftell(wk->logfile) << endl;

	//從vlog取值這邊有問題  用了會卡double free  已解決
	//從外面傳str進來接值
	fread(&value,length,1,wk->logfile);
	fseek(wk->logfile,offset-32,SEEK_SET);
	fread(&key,length,1,wk->logfile);
	//rewind(wk->logfile);
	cout << "Value Key: " <<key<<endl;
	cout << "LogFile Value: " << value << endl;
	return true;
}	

static void wisckey_set(WK * wk, string &key, string &value)
{
	long offset = ftell(wk->logfile);
	long size = sizeof(key);
	fwrite (&key, size,1,wk->logfile);
	offset = ftell(wk->logfile);
	size = sizeof(value);
	
	std::string vlog_offset = std::to_string(offset);
	std::string vlog_size = std::to_string(size);
	std::stringstream vlog_value;
	vlog_value << vlog_offset << "&&" << vlog_size;//紀錄偏移量還有size等等就知道要從哪裡取value值(get會用到)
	std::string s = vlog_value.str();
	
	fwrite (&value, size,1,wk->logfile);//寫到vlog裡面
	//更新head
	leveldb_set(wk->leveldb,key,s);//寫到levelDB裡面
	std::string headvalue = std::to_string(offset+size);
	leveldb_set(wk->leveldb,headkey,headvalue);
}

static void wisckey_del(WK * wk, string &key)
{
	//純粹從level DB刪掉資料  gc？？？
 	cout << "Key: " << key << endl; 
	leveldb_del(wk->leveldb,key);
}

static WK * open_wisckey(const string& dirname)
{
	WK * wk = new WK;
	wk->logfile = fopen("logfile","wb+");
	logfile = wk->logfile;
	wk->leveldb = open_leveldb(dirname,wk->logfile);
	db = wk->leveldb;
  	wk->dir = dirname;
  	return wk;
}

static void close_wisckey(WK * wk)
{
	fclose(wk->logfile);
  	delete wk->leveldb;
  	delete wk;
}




// For testing wisckey functionality 
static void testing_function2(WK * wk,string &key,string &value,string &accvalue) 
{
/* Setting Value and Testing it */     
	
	cout << "\n\n\t\tInput Received\n" << endl;
	cout << "Key: " << key << endl;
        cout << "Value: " << value << endl;
	wisckey_set(wk,key,value);
	std::string key2 = key+"_v2";
	std::string value2 = value+"_v2";
	std::string key3 = key+"_v3";
	std::string value3 = value+"_v3";
	std::string key4 = key+"_v4";
	std::string value4 = value+"_v4";
	wisckey_set(wk,key2,value2);

	const bool found = leveldb_get(wk->leveldb,headkey,accvalue);
	if (found) {
		cout << "Wisckey head :"<< accvalue << endl;
	}
	const bool found2 = wisckey_get(wk,key2,accvalue);
	if (found2) {
		cout << "Record Matched :"<< accvalue << endl;
	}
/* Deleting Value */
	cout << "\n\n\t\tDelete Operation\n" << endl;
	std::string del_key = key2;
	wisckey_del(wk,del_key);
	cout << "Delete Successful" << endl;
	wisckey_set(wk,key3,value3);
	wisckey_set(wk,key4,value4);
	const bool found3 = wisckey_get(wk,key3,accvalue);
	if (found3) {
		cout << "Record Matched :"<< accvalue << endl;
	}
	const bool found4 = wisckey_get(wk,key4,accvalue);
	if (found4) {
		cout << "Record Matched :"<< accvalue << endl;
	}
	const bool found5 = wisckey_get(wk,key2,accvalue);
	if (found5) {
		cout << "Record Matched :"<< accvalue << endl;
	}
	const bool hfound = leveldb_get(wk->leveldb,headkey,accvalue);
	if (hfound) {
		cout << "head now is:"<< accvalue << endl;
	}
	std::string input;
	//scanf("%s",input);
/*
// Read after Delete 
	cout << "\n\n\t\tInput Received\n" << endl;
        std::string testkey= key3;
        cout << "Key: " << testkey << endl;
        cout << accvalue <<endl;
	const bool testfound = wisckey_get(wk,testkey,accvalue);
        if (testfound) {
               cout << "Record Matched :"<< accvalue << endl;
        }*/

}	
static void testing_compaction(WK * wk,string &key,string &value,string &accvalue) 
{
/* Setting Value and Testing it */     
	cout << "\n\n\t\tInput Received\n" << endl;
	cout << "Key: " << key << endl;
        cout << "Value: " << value << endl;
        std::string temp;
        for(int i=0;i<2000;i++){
        	//cout << i <<endl;
        	key = key+"v"+std::to_string(i+1);
        	temp = value+"v"+std::to_string(i+1);
        	wisckey_set(wk,key,value);
        }
}

int main(int argc, char ** argv)
{
	if (argc < 2) {
    		cout << "Usage: " << argv[0] << " <value-size>" << endl;
    		exit(0);
  	}
  	const size_t value_size = std::stoull(argv[1], NULL, 10);
  	if (value_size < 1 || value_size > 100000) {
    		cout << "  <value-size> must be positive and less then 100000" << endl;
   	 	exit(0);
  	}
  	WK * wk = open_wisckey("wisckey_test_dir");
  	if (wk == NULL) {
    		cerr << "Open WiscKey failed!" << endl;
    		exit(1);
  	}
  	
  	/*
  	這裡是測試get set delete 都OK
  	*/
  	std::string testkey = "key";
  	std::string testvalue = "test";
  	std::string avalue;
  	testing_function2(wk,testkey,testvalue,avalue);
  	/*
  	這裡是測試
  	*/
	char * vbuf = new char[value_size];
  	for (size_t i = 0; i < value_size; i++) {
    		vbuf[i] = rand();
  	}
  	string value = string(vbuf, value_size);
 	size_t nfill = 1000000000 / (value_size + 8);
  	clock_t t0 = clock();
  	size_t p1 = nfill / 40;
  	for (size_t j = 0; j < nfill; j++) {
    		string key = std::to_string(((size_t)rand())*((size_t)rand()));
    		wisckey_set(wk, key, value);
   		if (j >= p1) {
      			clock_t dt = clock() - t0;
      			cout << "progress: " << j+1 << "/" << nfill << " time elapsed: " << dt * 1.0e-6 << endl << std::flush;
      			p1 += (nfill / 40);
    		}    
  	}

  	clock_t dt = clock() - t0;
  	cout << "time elapsed: " << dt * 1.0e-6 << " seconds" << endl;
        //close_wisckey(wk);
        destroy_leveldb("wisckey_test_dir");       
        remove("logfile");
        exit(0);
}
