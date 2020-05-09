#ifndef _TEMPLATEDB_DB_H_
#define _TEMPLATEDB_DB_H_

#include "def.h"

#include "logClass.h"

#include "mutexHelp.h"

#include "bloomFilter.h"

namespace templatedb
{

template<class T>
static inline void getdt(T& t, const char* addr, int ct, int& pos){
	memcpy(&t, addr + pos, ct*sizeof(T));
	pos += sizeof(T)*ct;
}
template<class T>
static inline void setdt(const T& t, char* addr, int ct, int& pos){
	memcpy(addr + pos, &t, ct*sizeof(T));
	pos += sizeof(T)*ct;
}

class Value
{
public:
    std::vector<_vltp_> items;
	long ver;
	char state;
public:
	Value() { reset(); };
	void reset(){		
		items.assign(myPar::getInstance().V_num, 0);
		items.clear();
		ver = -1;
		state = -1;
	}
	void init(){
		items.resize(myPar::getInstance().V_num);
	}
	bool isDel(){
		return (items.size() == 0 || state == -1);
	}
    bool operator ==(Value const & other) const
    {
        return (items == other.items) && state == other.state;
    }
	bool operator !=(Value const & other) const
	{
		return (items != other.items);
	}


	bool operator <(Value const & other) const
	{
		return ver<other.ver;
	}
	std::string toStr() const{
		std::string tmp;
		for (auto i = 0; i < items.size(); i++){
			tmp += std::to_string(items[i]) + ":";
		}
		return tmp;
	}

	static inline int maxC(){
		return myPar::getInstance().block_sz / Value::sz();
	}
	static inline int sz(){
		return (sizeof(_kytp_)+sizeof(_vltp_)*myPar::getInstance().V_num + sizeof(long) + sizeof(char));
	}
};



enum dbTp{
	GNULL_ = 1,
	GYES_,
	PNO_,
	PYES_,
};

typedef std::map<_kytp_, Value> MEMTB;

struct Symdt{
	_kytp_ first;
	_kytp_ second;
	int count;

	int idx;
	long id;
	int delc;

	int offset;//block offset in disktable
};
struct SymCmp
{
	bool operator () (const Symdt& x, const Symdt& y) const
	{
		return x.first <= y.first;
	}
};
static bool SymCmp_id(const std::vector<Symdt>::iterator& x, const std::vector<Symdt>::iterator& y)
{
	return x->id > y->id;
}


class DBBase{
public:
	DBBase() = default;
public:
	virtual dbTp get(_kytp_ key, Value&) = 0;
	virtual dbTp put(_kytp_ key, Value& valm) = 0;
	virtual dbTp del(_kytp_ key) = 0;
	virtual dbTp upd(_kytp_ key, Value& val) = 0;
	virtual void scan(_kytp_ min_key, _kytp_ max_key, std::vector<Value>&) = 0;
	virtual std::size_t size() = 0;
};
class DB : public DBBase
{
public:
    DB(){
		mlogS::getInstance().write("db construct");
		table.clear();
		im_table.clear();
		im_flag = false;
		ditmp.clear();
		oidx = 0;

		dumpidx = 0;

		bf = new BloomFilter<_kytp_>(myPar::getInstance().bfsz, myPar::getInstance().errorRt);
		dumptmp.resize(myPar::getInstance().lockpool, NULL);
		for (int i = 0; i < myPar::getInstance().lockpool; i++){
			dumptmp[i] = new char[myPar::getInstance().block_sz];
		}
		dumptmpm1 = new char[myPar::getInstance().block_sz];
		dumptmpm2 = new char[myPar::getInstance().block_sz];
		dumptmpd = new char[myPar::getInstance().block_sz];
		mtxpl = new std::mutex[myPar::getInstance().lockpool];
		load();
		mlogS::getInstance().write("db construct done");
	}
	~DB(){
		mlogS::getInstance().write("db destroy");
		save();
		for (int i = 0; i < myPar::getInstance().lockpool; i++){
			if (dumptmp[i] != NULL){
				delete[] dumptmp[i];
				dumptmp[i] = NULL;
			}
		}
		if (dumptmpm1 != NULL){
			delete[] dumptmpm1;
			dumptmpm1 = NULL;
		}
		if (dumptmpm2 != NULL){
			delete[] dumptmpm2;
			dumptmpm2 = NULL;
		}
		if (dumptmpd != NULL){
			delete[] dumptmpd;
			dumptmpd = NULL;
		}
		if (bf != NULL){
			delete bf;
			bf = NULL;
		}
		if (NULL != mtxpl){
			delete []mtxpl;
			mtxpl = NULL;
		}
		mlogS::getInstance().write("db destroy done");
	}

	void save();
	void load();
	void recoverLog();

	dbTp get(_kytp_ key, Value& v){
		return get(key, v, 1);
	}
	dbTp put(_kytp_ key, Value& v){
		return put(key, v, 1);
	}
	dbTp upd(_kytp_ key, Value& v){
		return upd(key, v, 0);
	}
	dbTp del(_kytp_ key);
	void scan(_kytp_ min_key, _kytp_ max_key, std::vector<Value>&);
	std::size_t size(){
		//mempool
		std::vector<Value> tmp;
		scan((std::numeric_limits<_kytp_>::min)(), (std::numeric_limits<_kytp_>::max)(), tmp);
		return tmp.size();
	}


	int binarySearch(std::vector<Symdt>::iterator, _kytp_, Value&, int);
	void ibinarySearch(std::vector<Symdt>&, std::vector< std::vector<Symdt>::iterator>&, _kytp_, bool issc = false);


	void refreshBF();

	void removeDiskTable(int);
	void doMerge(int, int);
	void doDelete(int);
	void dumpMemTable();
	void loadDiskTable(int idx, char*);
	void saveDiskTable(int idx, char*);
	void saveIndex();

private:
	dbTp get(_kytp_ key, Value &, int s);
	dbTp put(_kytp_ key, Value& val, int s);
	dbTp upd(_kytp_ key, Value& val, int s);

public:
	logClass lc;
	MEMTB table;
	MEMTB im_table;
	bool im_flag;

	std::atomic<int> dumpidx;

	std::mutex* mtxpl;
	std::vector<char*> dumptmp;
	char* dumptmpm1;
	char* dumptmpm2;
	char* dumptmpd;

	BloomFilter<_kytp_> *bf;

	std::vector< std::pair<_kytp_, Value> > tmpm, tmpmt;
	
	
	std::vector<Symdt> ditmp;
	std::atomic<int> oidx;

	WfirstRWLock idbmtx;
	WfirstRWLock dbmtx;
};
typedef Singleton<DB> CDB;

}   // namespace templatedb

#endif 