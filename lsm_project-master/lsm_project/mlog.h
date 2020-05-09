#ifndef MLOG_H
#define MLOG_H

#include "def.h"

static std::mutex logmtx;


class mlog
{
public:
    mlog();

    void write(const std::string& str);

    ~mlog();

    std::ofstream ofs;

};

typedef Singleton<mlog> mlogS;

#endif // MLOG_H
