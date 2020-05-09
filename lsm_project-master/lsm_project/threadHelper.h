#pragma once

#include "def.h"
#include "db.hpp"


#define maxab(x,y)  ((x)<(y)?(y):(x))
#define minab(x,y)  ((x)>(y)?(y):(x))
class loopThread{
public:
	loopThread(){
		isrun = false;
	}
	void run(){
		isrun = true;
		while (isrun){

			struct tm *newtime;
			time_t long_time;
			time(&long_time);
			newtime = localtime(&long_time);
			if (newtime->tm_hour == myPar::getInstance().maintainBeginTime){
				myPar::getInstance().spause = true;
			}


			mlogS::getInstance().write("maintain bloom filter...");
			if (myPar::getInstance().spause){
				std::this_thread::sleep_for(std::chrono::milliseconds(myPar::getInstance().slptm));
				templatedb::CDB::getInstance().refreshBF();
				myPar::getInstance().spause = false;
			}
			mlogS::getInstance().write("maintain bloom filter done");


			
			mlogS::getInstance().write("do disk merge...");
			int i = 0 ,sz = 0;
			while (true){
				templatedb::CDB::getInstance().idbmtx.lock_write();
				sz = templatedb::CDB::getInstance().ditmp.size();
				if (i < sz - 1){
					if (templatedb::CDB::getInstance().ditmp[i].second >= templatedb::CDB::getInstance().ditmp[i + 1].first || 
						(templatedb::CDB::getInstance().ditmp[i].count + templatedb::CDB::getInstance().ditmp[i + 1].count <= templatedb::Value::maxC() 
						|| templatedb::CDB::getInstance().ditmp[i].count < templatedb::Value::maxC() * myPar::getInstance().fsz)){
						templatedb::CDB::getInstance().doMerge(i, i+1);
					}
				}
				else{
					templatedb::CDB::getInstance().idbmtx.release_write();
					break;
				}
				templatedb::CDB::getInstance().idbmtx.release_write();
				i++;
				std::this_thread::sleep_for(std::chrono::milliseconds(myPar::getInstance().slptm));
			}	
			mlogS::getInstance().write("do disk merge done");


			
			mlogS::getInstance().write("do disk delete...");
			i = 0;
			sz = 0;
			bool ol;
			int j;
			while (true){
				templatedb::CDB::getInstance().idbmtx.lock_write();
				sz = templatedb::CDB::getInstance().ditmp.size();
				if (i < sz){
					ol = true;
					for (j = 0; j < sz; j++){
						if (i != j && maxab(templatedb::CDB::getInstance().ditmp[i].first, templatedb::CDB::getInstance().ditmp[j].first)
							< minab(templatedb::CDB::getInstance().ditmp[i].second, templatedb::CDB::getInstance().ditmp[j].second)){
							ol = false;
							break;
						}
					}
					if (ol && templatedb::CDB::getInstance().ditmp[i].delc > templatedb::Value::maxC() * myPar::getInstance().delr){
						templatedb::CDB::getInstance().doDelete(i);
					}
				}
				else{
					templatedb::CDB::getInstance().idbmtx.release_write();
					break;
				}
				templatedb::CDB::getInstance().idbmtx.release_write();
				i++;
				std::this_thread::sleep_for(std::chrono::milliseconds(myPar::getInstance().slptm));
			}
			mlogS::getInstance().write("do disk delete done");
			

			std::this_thread::sleep_for(std::chrono::seconds(myPar::getInstance().roolTm));
		}
	}
	void start(){
		std::thread thr(std::bind(&loopThread::run, this));
		this->th = std::move(thr);
		//this->th = std::thread(std::bind(&loopThread::run, this));
	}
	void stop(){
		isrun = false;
	}
	void wait(){
		if (th.joinable())
			th.join();
	}
	~loopThread(){
		stop();
		wait();
	}
public:
	volatile bool isrun;
private:
	std::thread  th;
};
typedef Singleton<loopThread> lThread;