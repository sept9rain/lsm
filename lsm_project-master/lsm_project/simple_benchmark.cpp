#include <iostream>
#include <chrono>

#include "def.h"

#include "mlog.h"

#include "db.hpp"


static int run_btest(PARAMS* par)
{

	std::default_random_engine dre(std::time(0));
	std::uniform_int_distribution<int> d(0, par->T_num - 1);



	std::string exePath = getEPath();
	templatedb::DBBase* ptr = (templatedb::DBBase*)par->cdb;

	std::vector<templatedb::Value> vl;
	vl.resize(par->T_num);
	std::unordered_set<int> us;
	templatedb::Value v, vt;


	
	for (int ii = 0; ii < par->T_num; ii++){
		ptr->get(ii, v);
		if (v.isDel()){
			printMe("get not exist", PWM);
		}
		else{
			printMe("get exist", PWM);
		}
	}
	


	int tmp;
	for (int ii = 0; ii < par->T_num; ii++){
		v.reset();
		for (auto j = 0; j < par->V_num; j++){
			if (j == 0){
				tmp = 0;
				tmp |= 'a';
				tmp <<= 8;
				tmp |= 'b';
				tmp <<= 8;
				tmp |= 'c';
				tmp <<= 8;
				tmp |= 'd';
				tmp <<= 8;
				v.items.push_back(tmp);
			}
			else
				v.items.push_back(ii + 1);
		}
		vl[ii] = v;
		templatedb::dbTp tp = ptr->put(ii, v);

#ifdef MDEBUG
		if (templatedb::PNO_ == tp){
			printMe("not insert", PWM);
		}
		else{
			printMe("insert", PWM);
		}
#endif

	}

	std::vector<templatedb::Value> vv;
	ptr->scan(0, par->T_num, vv);

#ifdef MDEBUG
	if (vv.size() != vl.size()){
		std::cout << vv.size() << " != " << vl.size() << std::endl;
		printMe("vector not equal", PWM);
		SYSPAUSE
	}
	std::set<templatedb::Value> mvv;
	for (int i = 0; i < vv.size(); i++){
		mvv.insert(vv[i]);
	}
#endif


	auto start = std::chrono::high_resolution_clock::now();
	for (int i = 0; i < par->T_num; i++){
		ptr->get(i, v);

#ifdef MDEBUG
		if (v != vl[i]){
			printMe("find value not equal", PWM);
			std::cout << v.toStr() << " != " << vl[i].toStr() << std::endl;
			if (v.toStr().find("0:0:0:0") != std::string::npos){
				printMe("find del error", PWM);
				SYSPAUSE
			}
		}
		else{
			printMe("find value equal", PWM);
		}
		if (mvv.find(v) == mvv.end()){
			printMe("scan error", PWM);
			SYSPAUSE
		}
#endif


		if (i % 3 == 0){
			v.reset();
			for (auto j = 0; j < par->V_num; j++){
				if (j == 0){
					tmp = 0;
					tmp |= 'a';
					tmp <<= 8;
					tmp |= 'b';
					tmp <<= 8;
					tmp |= 'c';
					tmp <<= 8;
					tmp |= 'd';
					tmp <<= 8;
					v.items.push_back(tmp);
				}
				else
					v.items.push_back(random(0, 1e6));
			}
			ptr->upd(i, v);
#ifdef MDEBUG
			vt = v;
			ptr->get(i, v);
			if ((v == vl[i] || vt != v)){
				printMe("find update error", PWM);
				std::cout << v.toStr() << " " << vt.toStr() << " " << vl[i].toStr() << std::endl;
				SYSPAUSE
			}
#endif
		}


		if ((i + 1) % 3 == 0){
			ptr->del(i);
#ifdef MDEBUG
			ptr->get(i, v);
			if (!v.isDel()){
				printMe("find value not empty", PWM);
				std::cout << v.toStr() << std::endl;
				SYSPAUSE
			}
#endif
		}

	}



	for (int ii = 0; ii < par->T_num; ii++){
		ptr->get(ii, v);
		if (v.isDel()){
			printMe("get not exist", PWM);
		}
		else{
			printMe("get exist", PWM);
		}
	}



	auto end = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();


	printMe(("Workload Time " + std::to_string(duration) + "us").c_str(), PPM);

	return 0;
}
