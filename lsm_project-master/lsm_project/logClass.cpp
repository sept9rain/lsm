#include "logClass.h"


void logClass::init(bool f){
	mlogS::getInstance().write("log_backup init");
	if (f){
		if (isFile(lfn.c_str())){
			copyFile(lfn.c_str(), bk.c_str());
		}
		lf = fopen(lfn.c_str(), "w");
	}
	else{
		lf = fopen(lfn.c_str(), "a+");
	}
	if (!lf){
		mlogS::getInstance().write("log_backup open error");
		printMe("log_backup open error", PEM);
	}
	buff = "";

	mlogS::getInstance().write("log_backup init done");
}

void logClass::write(const std::string& str, bool s){
	buff += str;
	if (s && buff.size() >= RWBSZ){
		fwrite(buff.c_str(), sizeof(char), RWBSZ, lf);
		fflush(lf);
		buff = buff.substr(RWBSZ);
	}
	if(!s){
		fwrite(buff.c_str(), sizeof(char), buff.length(), lf);
		fflush(lf);
	}
}

logClass::logClass()
{
	lfn = getEPath() + "/" + myPar::getInstance().dbName + "_backup.log";
	bk = getEPath() + "/" + myPar::getInstance().dbName + "_backup_copy.log";
	lf = NULL;
	buff = "";
}


logClass::~logClass()
{
	if (NULL != lf){
		fclose(lf);
		lf = NULL;
	}
}
