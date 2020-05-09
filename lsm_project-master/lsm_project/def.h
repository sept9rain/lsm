#pragma once
#pragma warning(disable:4996)



//c++ header
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <string>
#include <exception>
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>
#include <thread>
#include <ctime>
#include <mutex>
#include <chrono>
#include <algorithm>
#include <atomic>
#include <utility>
#include <limits>
#include <random>
#include <list>
#include <functional>



//c header
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <assert.h>




#ifdef WIN32
#define WIN32_LEAN_AND_MEAN  
//window header
#include <Windows.h>
#include <Psapi.h>
#include <direct.h> 
#include <io.h>

#ifdef _WIN64
#else
#endif
#else
#define _BSD_SOURCE
//linux/unix header
#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <sys/io.h>
#include <fcntl.h>
#include <libgen.h>
#include <dirent.h>

#ifdef __x86_64__ //x64
#elif __i386__ //x86
#endif
#endif


#define MDEBUG


#define VERSION "v1.1"


typedef long _kytp_;
typedef int _vltp_;
#define ISUNIQUE 0



const static int BitWide_ = 64;
#define FNSZ 512
#define BUF_SZ 1024
#define RWBSZ 4096


#define SYSPAUSE printf("Press Enter Any Key To Continue..."); fgetc(stdin);

template<typename T>
static inline bool isNormalNumber(T n){
	return !(std::isnan(n) || std::isinf(n));
}
static unsigned int random(double start, double end)
{
	return (unsigned int)(start + (end - start)*rand() / (RAND_MAX + 1.f));
}
static std::string getTime()
{
	time_t timep;
	time(&timep);
	char tmp[FNSZ];
	strftime(tmp, sizeof(tmp), "%Y-%m-%d %H:%M:%S", localtime(&timep));
	return std::string(tmp);
}
static void pmessage(const char* in){
	std::cout << getTime() << " Message : " << in << std::endl;
}
static void emessage(const char* in){
	std::cerr << getTime() << " Error Exit : " << in << std::endl;
	exit(-1);
}
static void wmessage(const char* in){
	std::cerr << getTime() << " Warning : " << in << std::endl;
}
static void amessage(const char* in){
	std::cerr << getTime() << " Abort : " << in << std::endl;
	abort();
}
enum PEUnion{
	PPM = 0,
	PEM,
	PWM,
	PAM
};
static void printMe(const char* in, PEUnion tp){
	switch (tp)
	{
	case PPM:
		pmessage(in);
		break;
	case PEM:
		emessage(in);
		break;
	case PWM:
		wmessage(in);
		break;
	case PAM:
		amessage(in);
		break;
	default:
		break;
	}
}

static void split(const  std::string &s, const std::string &delim, std::vector<std::string>& elems){
	elems.clear();
	size_t pos = 0;
	size_t len = s.length();
	size_t delim_len = delim.length();
	if (delim_len == 0) return;
	while (pos < len)
	{
		size_t find_pos = s.find(delim, pos);
		if (find_pos == std::string::npos)
		{
			elems.push_back(s.substr(pos, len - pos));
			break;
		}
		elems.push_back(s.substr(pos, find_pos - pos));
		pos = find_pos + delim_len;
	}
}
static std::string string_replace(const std::string &s, const std::string &s2, const std::string &s3)
{
	std::string s1 = s;
	std::string::size_type pos = 0;
	std::string::size_type a = s2.size();
	std::string::size_type b = s3.size();
	while ((pos = s1.find(s2, pos)) != std::string::npos)
	{
		s1.replace(pos, a, s3);
		pos += b;
	}
	return s1;
}
static void pathAFile(const std::string &s, std::string &path, std::string &file){
	if (s.find("\\") == std::string::npos && s.find("/") == std::string::npos){
		file = s;
		path = ".\\";
		return;
	}
	else if (s.find("\\") != std::string::npos){
		path = s.substr(0, s.rfind("\\"));
		file = s.substr(s.rfind("\\") + 1);
	}
	else if (s.find("/") != std::string::npos){
		path = s.substr(0, s.rfind("/"));
		file = s.substr(s.rfind("/") + 1);
	}
}
static std::string estring_replace(const std::string &s, const std::string &ss, const std::string& sss){
	if (s.find(ss) != std::string::npos){
		size_t pos = s.rfind(ss);
		return s.substr(0, pos) + sss;
	}
	else{
		return s + sss;
	}
}

#define FREEC(PTR) free(PTR); PTR=NULL
static inline void* xwbm(void *ptr, size_t sz) {//sz = n * sizeof(element)
	ptr = malloc(sz);
	assert(ptr != NULL);
	return ptr;
}
static inline void* xwbr(void *ptr, size_t sz) {//sz = n * sizeof(element)
	ptr = realloc(ptr, sz);
	assert(ptr != NULL);
	return ptr;
}
static inline void* xwbc(void *ptr, size_t n, size_t esz) {//sz = n*esz
	ptr = calloc(n, esz);
	assert(ptr != NULL);
	return ptr;
}
#define FREECPP(PTR) delete[] PTR; PTR=NULL
static inline void* xwbn(void *ptr, size_t sz) {//sz = n * sizeof(element)
	ptr = (void*)(new char[sz]);
	assert(ptr != NULL);
	return ptr;
}


static bool makeDir(const std::string& pathName)
{
#ifdef WIN32  
	if (::_mkdir(pathName.c_str()) < 0)
	{
		return false;
	}
#else  // Linux  
	if (::mkdir(pathName.c_str(), S_IRWXU | S_IRGRP | S_IXGRP) < 0)
	{
		return false;
	}
#endif  
	return true;
}
static void copyFile(const char* src, const char* dst)
{
	std::ifstream in(src, std::ios::binary);
	std::ofstream out(dst, std::ios::binary);
	if (!in.is_open()) {
		return;
	}
	if (!out.is_open()) {
		return;
	}
	if (src == dst) {
		return;
	}
	char buf[RWBSZ];
	while (in)
	{
		in.read(buf, RWBSZ);
		out.write(buf, in.gcount());
	}
	in.close();
	out.close();
}
static bool isFile(const char* file){
#ifdef WIN32	
	if ((_access(file, 0)) != -1)
#else
	if ((access(file, 0)) != -1)
#endif
	{
		return true;
	}
	return false;
}
static bool rmFile(const char*file){
	if (!std::remove(file) == 0)
	{
		return true;
	}
	return false;
}
static bool renameFile(const char* file, const char* newfile){
	if (!std::rename(file, newfile))
	{
		return false;
	}
	return true;
}

static int get_CPU_core_num()
{
#if defined(WIN32)
	SYSTEM_INFO info;
	GetSystemInfo(&info);
	return info.dwNumberOfProcessors;
#else
	return get_nprocs();
#endif
}

#ifdef _WIN32
static std::string getEPath()
{
	std::string exePath;
	pathAFile(_pgmptr, exePath, std::string(""));
	return exePath;
}
#else
static std::string getEPath()
{
	char processdir[FNSZ];
	char* filename;
	int   len = sizeof(processdir);
	if (readlink("/proc/self/exe", processdir, len) <= 0)
	{
		return std::string();
	}
	if (len >= 0)
	{
		processdir[len-1] = '/';
	}

	std::string exe_path = processdir;

	size_t nPos = exe_path.find_last_of("/");
	if (nPos > 0)
	{
		exe_path = exe_path.substr(0, nPos);
		exe_path.append("/");
	}
	return exe_path;
}
#endif



template<typename T, typename L = std::mutex>
class Singleton
{
public:
	template<typename ...Args>
	static T &getInstance(Args&&... args)
	{
		if (!m_init)
		{
			if (nullptr == m_inst)
			{
				m_lock.lock();
				m_inst = new T(std::forward<Args>(args)...);
				m_init = true;
				m_lock.unlock();
			}
		}
		return *m_inst;
	}

private:
	Singleton() {}
private:
	static bool m_init;
	static T    *m_inst;
	static L    m_lock;
};
template<typename T, typename L>
bool Singleton<T, L>::m_init = false;
template<typename T, typename L>
T *Singleton<T, L>::m_inst = nullptr;
template<typename T, typename L>
L Singleton<T, L>::m_lock;




static time_t getCT(){
	time_t timep;
	time(&timep); 
	return timep;
}

typedef struct myParameters{
	int threadNum;
	int mem;
	int lockpool;

	std::string option;	
	std::string exePath;
	std::string commendLine;

	int maintainBeginTime;

	const static int slptm = 10;
	const static int logSZ = 5;
	const static int block_sz = BitWide_ * 1024;

	float fsz;
	float delr;

	std::thread *sthr;
	std::atomic_bool spause;
	std::atomic_bool srun;

	int bfsz;
	double errorRt;

	int T_num;
	int V_num;

	void* cdb;

	int port;

	int roolTm;

	std::string dbName;


	void init(){
		T_num = 10000;		
		bfsz = 100000;
		V_num = 4;
		errorRt = 1e-4;

		mem = 2;
		threadNum = get_CPU_core_num();
		exePath = getEPath();
		lockpool = 10;

		fsz = 0.7;
		delr = 0.1;
		
		sthr = NULL;
		spause = false;
		srun = true;
		maintainBeginTime = 23;
		port = 2000;

		roolTm = 1;
		

		cdb = NULL;	
		dbName = "default";

		option = "";
		commendLine = "";

	}
	myParameters(){
		init();
	}
	std::string printp() const{
		std::stringstream re;
		re << "\n==================Parameters==================\n";
		re << "Option     : " << option << "\n";

		re << "K-V Num    : " << T_num << "\n";
		re << "Value Len  : " << V_num << "\n";

		re << "Thread Num : " << threadNum << "\n";
		re << "Memory     : " << mem << "\n";
		re << "Port       : " << port << "\n";
		re << "Command    : " << commendLine << "\n";
		re << "==================Parameters==================" << std::endl;
		return re.str();
	}
}PARAMS;
typedef Singleton<PARAMS> myPar;





#ifdef WIN32
static int GetSystemMemoryInfo()
{
	SYSTEM_INFO si;
	GetSystemInfo(&si);

	DWORD pid = GetCurrentProcessId();
	HANDLE handle;
	handle = ::OpenProcess(PROCESS_ALL_ACCESS, FALSE, pid);
	PROCESS_MEMORY_COUNTERS pmc;
	GetProcessMemoryInfo(handle, &pmc, sizeof(pmc));

	int usedMemory = 0;

	PSAPI_WORKING_SET_INFORMATION workSet;
	memset(&workSet, 0, sizeof(workSet));
	BOOL bOk = QueryWorkingSet(handle, &workSet, sizeof(workSet));
	if (bOk || (!bOk && GetLastError() == ERROR_BAD_LENGTH))
	{
		int nSize = sizeof(workSet.NumberOfEntries) + workSet.NumberOfEntries*sizeof(workSet.WorkingSetInfo);
		char* pBuf = new char[nSize];
		if (pBuf)
		{
			QueryWorkingSet(handle, pBuf, nSize);
			PSAPI_WORKING_SET_BLOCK* pFirst = (PSAPI_WORKING_SET_BLOCK*)(pBuf + sizeof(workSet.NumberOfEntries));
			DWORD dwMem = 0;
			for (ULONG_PTR nMemEntryCnt = 0; nMemEntryCnt < workSet.NumberOfEntries; nMemEntryCnt++, pFirst++)
			{
				if (pFirst->Shared == 0)
				{
					dwMem += si.dwPageSize;
				}
			}
			delete pBuf;
			pBuf = NULL;
			if (workSet.NumberOfEntries > 0)
			{
				usedMemory = dwMem / 1024;
			}
		}
	}
	CloseHandle(handle);
	return usedMemory;
}

#else
static int GetSystemMemoryInfo(){
	unsigned int pid = getpid();
	char file_name[FNSZ] = { 0 };
	FILE *fd;
	char line_buff[BUF_SZ] = { 0 };
	sprintf(file_name, "/proc/%d/status", pid);

	fd = fopen(file_name, "r");
	if (nullptr == fd){
		return 0;
	}

	char name[FNSZ];
	int vmrss;
	for (int i = 0; i< 16; i++){
		fgets(line_buff, sizeof(line_buff), fd);
	}

	fgets(line_buff, sizeof(line_buff), fd);
	sscanf(line_buff, "%s %d", name, &vmrss);
	fclose(fd);

	return vmrss;
}
#endif

class CountTime{
public:
	CountTime(){
		duration = 0;
		durmem = 0;
	}
public:
	void begin(){
		startmem = GetSystemMemoryInfo();

		_start = std::chrono::high_resolution_clock::now();
#ifdef WIN32
		QueryPerformanceCounter(&start);
#else
		gettimeofday(&start, NULL);
#endif 	
	}
	void oneEnd(){
		endmem = GetSystemMemoryInfo();
		durmem += endmem - startmem;

		_end = std::chrono::high_resolution_clock::now();
		duration += std::chrono::duration_cast<std::chrono::microseconds>(_end - _start).count();
#ifdef WIN32
		QueryPerformanceCounter(&end);
		//duration += (double)(end.QuadPart - start.QuadPart);
#else
		gettimeofday(&end, NULL);
		//duration += (double)(end.tv_usec - start.tv_usec) + (double)(end.tv_sec - start.tv_sec) * 1e6;
#endif 	
		
	}
	void print(std::string me){
		fprintf(stdout, "%s %s cost %dms %dk \n", getTime().c_str(), me.c_str(), (int)duration, durmem);
		duration = 0;
		durmem = 0;
	}
private:
#ifdef WIN32
	LARGE_INTEGER start, end;
#else
	struct timeval start, end;
#endif
	std::chrono::system_clock::time_point _start;
	std::chrono::system_clock::time_point _end;
	int startmem, endmem;
	double  duration;
	int durmem;
};
