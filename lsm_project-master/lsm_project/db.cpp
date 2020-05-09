#include "db.hpp"

using namespace templatedb;

void DB::ibinarySearch(std::vector<Symdt>&vt, std::vector< std::vector<Symdt>::iterator >& out, _kytp_ key, bool issc){
	out.clear();
	if (vt.size() == 0) return;
	Symdt t;
	t.first = key;	
	auto b = std::lower_bound(vt.begin(), vt.end(), t, SymCmp());
	for (auto i = vt.begin(); i != b; i++){
		if (issc){
			out.push_back(i);
		}
		else if (i->second >= key){
			out.push_back(i);
		}
	}
}
int DB::binarySearch(std::vector<Symdt>::iterator i, _kytp_ key, Value& vl, int setv){
	loadDiskTable(i->idx, dumptmp[i->idx % myPar::getInstance().lockpool]);
	Value vlt;
	_kytp_ ky, kyt;
	vlt.init();
	if (i->count <= 0){
		return -1;
	}
	int low = 0, high = i->count - 1, middle;
	int pos, post;
	while (low <= high){
		middle = (low + high) / 2;
		pos = Value::sz() * middle;
		getdt(ky, dumptmp[i->idx % myPar::getInstance().lockpool], 1, pos);
		if (setv > 1){
			if (ky >= key){
				if (middle < 1)
					return middle;
				post = Value::sz() * (middle - 1);
				getdt(kyt, dumptmp[i->idx % myPar::getInstance().lockpool], 1, post);
				if (kyt < key)
					return middle;
				high = middle-1;
			}
			else if (ky < key){
				low = middle + 1;
			}
		}
		else{
			if (key == ky){
				if (setv==1){
					getdt(vlt.items[0], dumptmp[i->idx % myPar::getInstance().lockpool], myPar::getInstance().V_num, pos);
					getdt(vlt.ver, dumptmp[i->idx % myPar::getInstance().lockpool], 1, pos);
					getdt(vlt.state, dumptmp[i->idx % myPar::getInstance().lockpool], 1, pos);
					if (vlt.ver > vl.ver){
						vl = vlt;
					}
				}
				else{
					setdt(vl.items[0], dumptmp[i->idx % myPar::getInstance().lockpool], myPar::getInstance().V_num, pos);
					setdt(vl.ver, dumptmp[i->idx % myPar::getInstance().lockpool], 1, pos);
					getdt(vlt.state, dumptmp[i->idx % myPar::getInstance().lockpool], 1, pos);
					pos -= sizeof(vlt.state);
					setdt(vl.state, dumptmp[i->idx % myPar::getInstance().lockpool], 1, pos);
					saveDiskTable(i->idx, dumptmp[i->idx % myPar::getInstance().lockpool]);
					if (vlt.state == -1){
						i->delc--;
					}
					if (vl.state == -1){
						i->delc++;
					}
				}
				return middle;
			}
			else if (key > ky){
				low = middle + 1;
			}
			else{
				high = middle-1;
			}
		}
	}
	return -1;
}



dbTp DB::get(_kytp_ key, Value &v, int setv)
{
	std::vector<std::vector<Symdt>::iterator> idxv;//mempool
	dbTp re = GNULL_;
	if (setv == 1)
		v = Value();
	if (bf->exists(key)){	

		dbmtx.lock_read();
		if (table.find(key) != table.end()){
			if (setv == 1){
				v = table[key];
			}
			else{
				table[key] = v;
			}
			re = GYES_;
		}
		else if (im_table.find(key) != im_table.end()){
			if (setv == 1){
				v = im_table[key];
			}
			else{
				im_table[key] = v;
			}
			re = GYES_;
		}
		dbmtx.release_read();
	
		if (re != GYES_){
			idbmtx.lock_read();
			ibinarySearch(ditmp, idxv, key);
			for (auto k = idxv.begin(); k != idxv.end(); k++){
				mtxpl[(*k)->idx % myPar::getInstance().lockpool].lock();
				if (binarySearch(*k, key, v, setv) >= 0){
					re = GYES_;
				}
				mtxpl[(*k)->idx % myPar::getInstance().lockpool].unlock();
			}
			idbmtx.release_read();
		}
		
	}
	if (setv == 1){
		if (v.isDel()) re = GNULL_;
	}
	return re;
}


dbTp DB::put(_kytp_ key, Value& val, int s)
{
	dbTp re = PNO_;	
	Value vl;
#if ISUNIQUE != 0
	if (get(key, vl) == GNULL_){
#else
	if (true){
#endif
		dbmtx.lock_write();
		if (s == 1){
			oidx++;
			val.ver = oidx;
			val.state = 2;
		}
		if (s > -2){
			lc.write("put:" + std::to_string(key) + ":" + std::to_string(val.ver)
				+ ":" + std::to_string((int)val.state) + ":" + val.toStr() + "$\n");
		}
		table[key] = val;
		re = PYES_;
		dumpMemTable();
		dbmtx.release_write();
		bf->insert(key);		
	}
	else{
		re = PNO_;
	}
	return re;
}
dbTp DB::del(_kytp_ key){
	Value v;
	v.init();
	memset(&v.items[0], 0, sizeof(_vltp_)*myPar::getInstance().V_num);
	oidx++;
	v.ver = oidx;
	v.state = -1;
	return upd(key, v, -1);
}
dbTp DB::upd(_kytp_ key, Value& val, int s){
	if (s == 0){
		oidx++;
		val.ver = oidx;
		val.state = 1;
	}
#if ISUNIQUE == 0
	return put(key, val, s);
#else
	if (s != -2){
		lc.write("upd:" + std::to_string(key) + ":" + std::to_string(val.ver)
			+ ":" + std::to_string((int)val.state) + ":" + val.toStr() + "$\n");
	}
	return get(key, val, s);
#endif
}

void DB::scan(_kytp_ min_key, _kytp_ max_key, std::vector<Value> &v)
{
	std::unordered_map<_kytp_, std::pair<long, int> > unq;///////mempool
	std::vector<std::vector<Symdt>::iterator>idxv;//mempool
	unq.clear();
	idxv.clear();
	v.clear();
	dbmtx.lock_read();
    for (auto pair: table)
    {
		if ((pair.first >= min_key) && (pair.first <= max_key)){
			if (!pair.second.isDel()){
				v.push_back(pair.second);
				unq[pair.first] = std::make_pair(pair.second.ver, v.size() - 1);
			}
		}
		if (pair.first > max_key)
			break;
    }
	for (auto pair : im_table)
	{
		if ((pair.first >= min_key) && (pair.first <= max_key)){
			if (!pair.second.isDel() && unq.find(pair.first) == unq.end()){
				v.push_back(pair.second);
				unq[pair.first] = std::make_pair(pair.second.ver, v.size() - 1);
			}
		}
		if (pair.first > max_key)
			break;
	}
	dbmtx.release_read();

	_kytp_ ky;
	Value vl;
	int j, pos;
	vl.init();
	idbmtx.lock_read();
	ibinarySearch(ditmp, idxv, max_key, true);
	for (auto k = idxv.begin(); k != idxv.end(); k++){
		if ((*k)->second >= min_key){
			mtxpl[(*k)->idx % myPar::getInstance().lockpool].lock();
			for (j = binarySearch(*k, min_key, vl, 2); j < (*k)->count; j++){
				pos = Value::sz() * j;
				getdt(ky, dumptmp[(*k)->idx % myPar::getInstance().lockpool], 1, pos);
				if (ky <= max_key){
					getdt(vl.items[0], dumptmp[(*k)->idx % myPar::getInstance().lockpool], myPar::getInstance().V_num, pos);
					getdt(vl.ver, dumptmp[(*k)->idx % myPar::getInstance().lockpool], 1, pos);
					getdt(vl.state, dumptmp[(*k)->idx % myPar::getInstance().lockpool], 1, pos);
					if (!vl.isDel()){
						if (unq.find(ky) == unq.end()){
							v.push_back(vl);
							unq[ky] = std::make_pair(vl.ver, v.size() - 1);
						}
						else if (vl.ver > unq[ky].first){
							v[unq[ky].second] = vl;
							unq[ky] = std::make_pair(vl.ver, unq[ky].second);
						}
					}
				}
				if (ky> max_key)
					break;
			}
			mtxpl[(*k)->idx % myPar::getInstance().lockpool].unlock();
		}
	}
	idbmtx.release_read();
}




void DB::save(){
	mlogS::getInstance().write("run save db");
	lc.write("", false);
	mlogS::getInstance().write("save bf");
	bf->save(myPar::getInstance().dbName);
	idbmtx.lock_read();
	saveIndex();
	idbmtx.release_read();
	mlogS::getInstance().write("run save db done");
}
void DB::load(){
	mlogS::getInstance().write("run load db");
	mlogS::getInstance().write("load bf");
	bf->load(myPar::getInstance().dbName);
		
	FILE* f = fopen((getEPath() + "/" + myPar::getInstance().dbName + "_index").c_str(), "rb");
	idbmtx.lock_write();
	ditmp.clear();
	if (f){
		int disz = 0;
		fread(&im_flag, sizeof(im_flag), 1, f);
		fread(&oidx, sizeof(oidx), 1, f);
		fread(&disz, sizeof(disz), 1, f);
		ditmp.resize(disz);
		fread(&ditmp[0], sizeof(Symdt), disz, f);
		fclose(f);
	}
	idbmtx.release_write();

	recoverLog();
	mlogS::getInstance().write("run load db done");
}
void DB::recoverLog(){
	mlogS::getInstance().write("run recoverLog");
	std::ifstream ifs, ifst;
	
	std::vector<std::string> vs;
	std::vector<std::string> ele;
	std::string line;
	_kytp_ key = 0;
	Value val;
	int i, j;

	if (im_flag){
		ifs.open(lc.bk);
		if (ifs){
			while (!ifs.eof()){
				std::getline(ifs, line);
				if (line.find('$') == line.npos) continue;
				vs.push_back(line);
			}
			ifs.close();
		}
		im_flag = false;
	}
	ifst.open(lc.lfn);
	if (ifst){
		while (!ifst.eof()){
			std::getline(ifst, line);
			if (line.find('$') == line.npos) continue;
			vs.push_back(line);
		}
		ifst.close();
	}

	for (i = 0; i < vs.size(); i++){
		split(vs[i], ":", ele);;
		key = std::stol(ele[1]);
		val.reset();
		val.ver = std::stol(ele[2]);
		val.state = std::stoi(ele[3]);
		for (j = 4; j < ele.size()-1; j++){
			val.items.push_back(stoi(ele[j]));
		}
		if (ele[0] == "put"){
			put(key, val, -3);
		}
		else if (ele[0] == "upd"){
			upd(key, val, -2);
		}
	}

	lc.init(false);
	mlogS::getInstance().write("run recoverLog done");
}


void DB::dumpMemTable(){
	if (table.size() != 0 && table.size() % Value::maxC() == 0){

		if (table.size() == Value::maxC()*myPar::getInstance().mem && im_table.size() == 0){
			im_table.swap(table);
			im_flag = true;
			lc.write("", false);
			lc.init();
			table.clear();
		}
		if (im_table.size() == 0) return;


		int j;
		bool flag;
		int currentIdx = 0;
		while (true){
			flag = true;
			for (j = 0; j < ditmp.size(); j++){
				if (currentIdx == ditmp[j].idx){
					flag = false;
					break;
				}
			}
			if (flag) break;
			currentIdx++;
		}

		int cs = 0;
		int pos = 0;
		int delc = 0;
		_kytp_ maxky, minky;
		maxky = (std::numeric_limits<_kytp_>::min)();
		minky = (std::numeric_limits<_kytp_>::max)();
		for (MEMTB::iterator i = im_table.begin(); i != im_table.end();){
			if (cs < Value::maxC()){
				setdt(i->first, dumptmpd, 1, pos);
				setdt(i->second.items[0], dumptmpd, myPar::getInstance().V_num, pos);
				setdt(i->second.ver, dumptmpd, 1, pos);
				setdt(i->second.state, dumptmpd, 1, pos);
				if (maxky < i->first) maxky = i->first;
				if (minky > i->first) minky = i->first;
				if (i->second.state == -1) delc++;
				im_table.erase(i++);
			}
			else{
				break;
			}
			cs++;
		}
		saveDiskTable(currentIdx, dumptmpd);
		


		Symdt sdt;
		sdt.first = minky;
		sdt.second = maxky;
		sdt.idx = currentIdx;
		sdt.count = Value::maxC();
		sdt.id = getCT();
		sdt.delc = delc;
		idbmtx.lock_write();
		ditmp.insert(std::upper_bound(ditmp.begin(), ditmp.end(), sdt, SymCmp()), sdt);	
		saveIndex();
		idbmtx.release_write();


		dumpidx = dumpidx + 1;

		bf->save(myPar::getInstance().dbName);
	}
}


void DB::saveIndex(){
	FILE* f = fopen((getEPath() + "/" + myPar::getInstance().dbName + "_index").c_str(), "wb");
	if (!f){
		mlogS::getInstance().write("save index error");
		printMe("save index file error", PEM);
	}
	int disz = ditmp.size();
	fwrite(&im_flag, sizeof(im_flag), 1, f);
	fwrite(&oidx, sizeof(oidx), 1, f);
	fwrite(&disz, sizeof(disz), 1, f);	
	fwrite(&ditmp[0], sizeof(Symdt), disz, f);
	fclose(f);
}
void DB::saveDiskTable(int idx, char* tmp){
	std::string dfile = getEPath() + "/" + myPar::getInstance().dbName + "_" + std::to_string(idx);
	FILE* f = fopen(dfile.c_str(), "wb");
	if (!f){
		mlogS::getInstance().write("save disk table error");
		printMe("save disk table error", PEM);
	}
	fwrite(tmp, sizeof(char), myPar::getInstance().block_sz, f);
	fclose(f);
}
void DB::loadDiskTable(int idx, char* tmp){
	std::string dfile = getEPath() + "/" + myPar::getInstance().dbName + "_" + std::to_string(idx);
	FILE* f = fopen(dfile.c_str(), "rb");
	if (!f){
		mlogS::getInstance().write("load disk table error");
		printMe("load disk table error", PEM);
	}
	//size_t size = 0;
	//fseek(f, 0L, SEEK_END);
	//size = ftell(f);
	fread(tmp, sizeof(char), myPar::getInstance().block_sz, f);
	fclose(f);
}




void DB::refreshBF(){
	//scan memtalbe and disktable, save bf
}

void DB::removeDiskTable(int idx){
	std::string dfile = getEPath() + "/" + myPar::getInstance().dbName + "_" + std::to_string(idx);
	rmFile(dfile.c_str());
}
void DB::doMerge(int i1, int i2){
	Symdt& t1 = ditmp[i1];
	Symdt t2 = ditmp[i2];

	loadDiskTable(t1.idx, dumptmpm1);
	loadDiskTable(t2.idx, dumptmpm2);

	std::pair<_kytp_, Value> tkv;
	tmpmt.clear();
	tmpm.clear();
	int c1 = 0, c2 = 0, i;
	int pos1, pos2, pos;
	_kytp_ ky1, ky2;
	Value vl1, vl2;
	vl1.init();
	vl2.init();

	while (c1 < t1.count && c2 < t2.count){
		pos1 = c1*Value::sz();
		pos2 = c2*Value::sz();
		getdt(ky1, dumptmpm1, 1, pos1);
		getdt(ky2, dumptmpm2, 1, pos2);
		if (ky1 <= ky2){
			getdt(vl1.items[0], dumptmpm1, myPar::getInstance().V_num, pos1);
			getdt(vl1.ver, dumptmpm1, 1, pos1);
			getdt(vl1.state, dumptmpm1, 1, pos1);
			tmpm.push_back(std::make_pair(ky1, vl1));
			c1++;
		}
		else{
			getdt(vl2.items[0], dumptmpm2, myPar::getInstance().V_num, pos2);
			getdt(vl2.ver, dumptmpm2, 1, pos2);
			getdt(vl2.state, dumptmpm2, 1, pos2);
			tmpm.push_back(std::make_pair(ky2, vl2));
			c2++;
		}
	}
	while (c1 < t1.count){
		pos1 = c1*Value::sz();
		getdt(ky1, dumptmpm1, 1, pos1);
		getdt(vl1.items[0], dumptmpm1, myPar::getInstance().V_num, pos1);
		getdt(vl1.ver, dumptmpm1, 1, pos1);
		getdt(vl1.state, dumptmpm1, 1, pos1);
		tmpm.push_back(std::make_pair(ky1, vl1));
		c1++;
	}
	while (c2 < t2.count){
		pos2 = c2*Value::sz();
		getdt(ky2, dumptmpm2, 1, pos2);
		getdt(vl2.items[0], dumptmpm2, myPar::getInstance().V_num, pos2);
		getdt(vl2.ver, dumptmpm2, 1, pos2);
		getdt(vl2.state, dumptmpm2, 1, pos2);
		tmpm.push_back(std::make_pair(ky2, vl2));
		c2++;
	}

	if (tmpm.size() > 1){
		tkv = tmpm[0];
		tmpmt.push_back(tkv);
		for (i = 1; i < tmpm.size(); i++){
			if (tkv.first != tmpm[i].first){
				tkv = tmpm[i];
				tmpmt.push_back(tkv);
			}
			else if (tmpm[i].second.ver > tkv.second.ver){
				tkv = tmpm[i];
				tmpmt[tmpmt.size() - 1] = tkv;
			}
		}
		tmpmt.swap(tmpm);
	}


	int mink1, maxk1, mink2, maxk2, ct1 = 0, ct2 = 0, delc1 = 0, delc2 = 0;
	maxk1 = (std::numeric_limits<_kytp_>::min)();
	mink1 = (std::numeric_limits<_kytp_>::max)();
	maxk2 = (std::numeric_limits<_kytp_>::min)();
	mink2 = (std::numeric_limits<_kytp_>::max)();
	int sz = Value::maxC();
	for (i = 0; i < tmpm.size(); i++){
		if (i < sz){
			if (tmpm[i].first > maxk1){
				maxk1 = tmpm[i].first;
			}
			if (tmpm[i].first < mink1){
				mink1 = tmpm[i].first;
			}
			pos = ct1 * Value::sz();
			setdt(tmpm[i].first, dumptmpm1, 1, pos);
			setdt(tmpm[i].second.items[0], dumptmpm1, myPar::getInstance().V_num, pos);
			setdt(tmpm[i].second.ver, dumptmpm1, 1, pos);
			setdt(tmpm[i].second.state, dumptmpm1, 1, pos);
			ct1++;
			if (tmpm[i].second.state == -1)delc1++;
		}
		else{
			if (tmpm[i].first > maxk2){
				maxk2 = tmpm[i].first;
			}
			if (tmpm[i].first < mink2){
				mink2 = tmpm[i].first;
			}
			pos = ct2 * Value::sz();
			setdt(tmpm[i].first, dumptmpm2, 1, pos);
			setdt(tmpm[i].second.items[0], dumptmpm2, myPar::getInstance().V_num, pos);
			setdt(tmpm[i].second.ver, dumptmpm2, 1, pos);
			setdt(tmpm[i].second.state, dumptmpm2, 1, pos);
			ct2++;
			if (tmpm[i].second.state == -1)delc2++;
		}
	}
	
	t1.first = mink1;
	t1.second = maxk1;
	t1.count = ct1;
	t1.delc = delc1;
	saveDiskTable(t1.idx, dumptmpm1);

	t2.first = mink2;
	t2.second = maxk2;
	t2.count = ct2;
	t2.delc = delc2;
	if (t2.count == 0){
		removeDiskTable(t2.idx);
		ditmp.erase(ditmp.begin() + i2);	
	}
	else{
		ditmp.erase(ditmp.begin() + i2);
		ditmp.insert(std::upper_bound(ditmp.begin(), ditmp.end(), t2, SymCmp()), t2);
		saveDiskTable(t2.idx, dumptmpm2);
	}
	saveIndex();
}
void DB::doDelete(int id){
	loadDiskTable(ditmp[id].idx, dumptmpm1);
	int pos, i;
	_kytp_ ky;
	Value vl;
	vl.init();
	tmpm.clear();
	for (i = 0; i < ditmp[id].count; i++){
		pos = i*Value::sz();
		getdt(ky, dumptmpm1, 1, pos);
		getdt(vl.items[0], dumptmpm1, myPar::getInstance().V_num, pos);
		getdt(vl.ver, dumptmpm1, 1, pos);
		getdt(vl.state, dumptmpm1, 1, pos);
		if (vl.state != -1){
			tmpm.push_back(std::make_pair(ky, vl));
		}
	}
	int mink1, maxk1;
	maxk1 = (std::numeric_limits<_kytp_>::min)();
	mink1 = (std::numeric_limits<_kytp_>::max)();
	if (tmpm.size() == 0){
		removeDiskTable(ditmp[id].idx);
		ditmp.erase(ditmp.begin() + id);
	}
	else if (tmpm.size() < ditmp[id].count){
		for (i = 0; i < tmpm.size(); i++){
			if (tmpm[i].first > maxk1){
				maxk1 = tmpm[i].first;
			}
			if (tmpm[i].first < mink1){
				mink1 = tmpm[i].first;
			}
			pos = i*Value::sz();
			setdt(tmpm[i].first, dumptmpm1, 1, pos);
			setdt(tmpm[i].second.items[0], dumptmpm1, myPar::getInstance().V_num, pos);
			setdt(tmpm[i].second.ver, dumptmpm1, 1, pos);
			setdt(tmpm[i].second.state, dumptmpm1, 1, pos);
		}
		saveDiskTable(ditmp[id].idx, dumptmpm1);

		ditmp[id].count = tmpm.size();
		ditmp[id].first = mink1;
		ditmp[id].second = maxk1;
		ditmp[id].delc = 0;
		saveIndex();
	}
}