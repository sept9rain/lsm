#pragma once

#include "def.h"
#include "mlog.h"

class logClass
{
public:
	logClass();
	~logClass();
	void init(bool f = true);
	void write(const std::string& str, bool s = true);
	FILE* lf;
	std::string lfn;
	std::string bk;
	std::string buff;
};

