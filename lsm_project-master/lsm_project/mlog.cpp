#include "mlog.h"

mlog::mlog()
{
	int i = 0;
	std::string fname;
	std::string fname_now;
	while(true){
		fname = getEPath() + "/log_" + std::to_string(i % myPar::getInstance().logSZ) + ".txt";
		fname_now = getEPath() + "/log_" + std::to_string(i % myPar::getInstance().logSZ) + "_now.txt";

		if (!isFile(fname.c_str()) && !isFile(fname_now.c_str())){
			if (isFile(fname.c_str()))
				rmFile(fname.c_str());
			ofs.open(fname_now);
			if (!ofs) printMe("build log error", PEM);
			break;
		}
		if (isFile(fname_now.c_str())){
			if (isFile(fname.c_str()))
				rmFile(fname.c_str());
			renameFile(fname_now.c_str(), fname.c_str());
			fname = getEPath() + "/log_" + std::to_string((i + 1) % myPar::getInstance().logSZ) + ".txt";
			if (isFile(fname.c_str()))
				rmFile(fname.c_str());
			fname_now = getEPath() + "/log_" + std::to_string((i + 1) % myPar::getInstance().logSZ) + "_now.txt";
			ofs.open(fname_now);
			if (!ofs) printMe("build log error", PEM);
			break;
		}	
		if (i >= myPar::getInstance().logSZ){
			fname = getEPath() + "/log_0.txt";
			fname_now = getEPath() + "/log_0_now.txt";
			if (isFile(fname.c_str()))
				rmFile(fname.c_str());
			ofs.open(fname_now);
			if (!ofs) printMe("build log error", PEM);
			break;
		}

		i++;
	}
}

mlog::~mlog()
{
    ofs.close();
}


void mlog::write(const std::string& str){

    logmtx.lock();

    ofs << getTime() << " " << str <<std::endl;
    //ofs<<std::flush;

    logmtx.unlock();

}
