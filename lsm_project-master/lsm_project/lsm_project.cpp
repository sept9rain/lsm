

#include "def.h"
#include "mlog.h"

#include "db.hpp"

#include "threadHelper.h"

#include "simple_benchmark.cpp"


void Help(std::string pro){
	std::cout << "Help Information" << std::endl;
	std::cout << "Usage :\n\t--"<<pro<<" [OPTION] [PARAMETERS]" << std::endl;
	std::cout << "\tOPTION :" << std::endl;
	std::cout << "\ttest        : run all test" << std::endl;
	std::cout << "\tserver      : wait for client to execute SQL, use socket\n" << std::endl;

	std::cout << "\ttest   : PARAMEERS" << std::endl;
	std::cout << "\t-N INT      input key-value number[10000]" << std::endl;
	std::cout << "\t-L INT      input value length[4]" << std::endl;
	std::cout << "\t-T INT      input thread number[currentCore]" << std::endl;

	std::cout << "\tserver : PARAMEERS" << std::endl;
	std::cout << "\t-P INT      set service port[*]" << std::endl;

	std::cout << "\nExample :"<<pro<<" test -n 10000 -l 4\n";

	std::cout << "Version : " << VERSION << std::endl;
	std::cout << "Contact : xxx<XXX@gmail.com>\n";
}
 
bool getopt(int argc, char* argv[]){

	mlogS::getInstance().write("getopt");

	//need to complete

	int nargc = 2;
	for (int i = 0; i < argc; i++){
		myPar::getInstance().commendLine += argv[i];
		if (i < argc - 1)
			myPar::getInstance().commendLine += "\t";
	}

	try{

		if (argc >= nargc)
		{
			myPar::getInstance().option = argv[1];
		}
		else
		{
			myPar::getInstance().option = "server";
			return true;
		}

		if (myPar::getInstance().option == "test"){

			if (argc > nargc)
			{
				short i = nargc;
				while (*argv[i] == '-')
				{
					switch (*(argv[i] + 1))
					{
					case 'N':
					case 'n': i++; if (i >= argc){ throw "Address Exception"; } sscanf(argv[i], "%d", &myPar::getInstance().T_num);
						break;

					case 'L':
					case 'l': i++; if (i >= argc){ throw "Address Exception"; } sscanf(argv[i], "%d", &myPar::getInstance().V_num);
						break;

					case 'T':
					case 't': i++; if (i >= argc){ throw "Address Exception"; } sscanf(argv[i], "%d", &myPar::getInstance().threadNum);
						break;

					default:
						break;
					}
					if (++i >= argc) break;
				}
			}
		}
		else if (myPar::getInstance().option == "server"){

			if (argc > nargc)
			{
				short i = nargc;
				while (*argv[i] == '-')
				{
					switch (*(argv[i] + 1))
					{
					case 'P':
					case 'p': i++; if (i >= argc){ throw "Address Exception"; } sscanf(argv[i], "%d", &myPar::getInstance().port);
						break;

					default:
						break;
					}
					if (++i >= argc) break;
				}
			}
			if (myPar::getInstance().port < 0 || myPar::getInstance().port>65535){
				Help(argv[0]);
				return false;
			}

		}
		else{
			Help(argv[0]);
			return false;
		}

	}
	catch (...){ 
		Help(argv[0]); 
		mlogS::getInstance().write("PARSE PARAMETERS INCORRECT!"); 
		printMe("PARSE PARAMETERS INCORRECT!", PEM); 
		return false; 
	}

	std::string confpar= myPar::getInstance().printp();
	mlogS::getInstance().write(confpar);
	printMe(confpar.c_str(), PPM);

	mlogS::getInstance().write("getopt done");

	return true;
}

void init(){
	//oprate log, error
	mlogS::getInstance().write("do init");

	std::srand(std::time(0));
	
#ifndef MDEBUG
	//print message, success, see message
	freopen((getEPath() + "/stdout.txt").c_str(), "w", stdout);
	freopen((getEPath() + +"/stderr.txt").c_str(), "w", stderr);
#endif

	myPar::getInstance().cdb = &templatedb::CDB::getInstance();

	mlogS::getInstance().write("do init done");
}

void destroy(void){
	mlogS::getInstance().write("do destroy");

	while (lThread::getInstance().isrun){
		lThread::getInstance().stop();
		lThread::getInstance().wait();
	}

	if (myPar::getInstance().cdb){	
		delete &templatedb::CDB::getInstance();
	}

	mlogS::getInstance().write("do destroy done");
}

#include "asio.hpp"
void serverRun(){
	mlogS::getInstance().write("serverRun");

	asio::io_service io_service;
	asio::ip::udp::socket udp_socket(io_service);
	asio::ip::udp::endpoint local_add(asio::ip::address::from_string("127.0.0.1"), myPar::getInstance().port);
	udp_socket.open(local_add.protocol());
	udp_socket.bind(local_add);

	char receive_str[BUF_SZ] = { 0 };
	std::string tmp;
	std::vector<std::string> vtmp, vv;
	_kytp_ key, key_;
	templatedb::Value vl;
	std::vector<templatedb::Value> vvl;
	int i;
	while (myPar::getInstance().srun)
	{
		while (myPar::getInstance().spause){
			std::this_thread::sleep_for(std::chrono::milliseconds(myPar::getInstance().slptm));
		}

		asio::ip::udp::endpoint  sendpoint;
		udp_socket.receive_from(asio::buffer(receive_str, 1024), sendpoint);
		tmp = sendpoint.address().to_string();
		split(receive_str, ":", vtmp);
		std::cout << receive_str << std::endl;
		if (vtmp.size() > 0){
			if (vtmp[0] == "get" && vtmp.size() == 2){
				key = std::stol(vtmp[1]);
				templatedb::CDB::getInstance().get(key, vl);
				udp_socket.send_to(asio::buffer(vl.toStr()), sendpoint);
			}
			else if (vtmp[0] == "put" && vtmp.size() == 3){
				key = std::stol(vtmp[1]);
				split(vtmp[2], ",", vv);
				vl.init();
				for (i = 0; i < vv.size(); i++){
					vl.items[i] = std::stoi(vv[i]);
				}
				templatedb::CDB::getInstance().put(key, vl);
				udp_socket.send_to(asio::buffer("insert success"), sendpoint);
			}
			else if (vtmp[0] == "del" && vtmp.size() == 2){
				key = std::stol(vtmp[1]);
				templatedb::CDB::getInstance().del(key);
				udp_socket.send_to(asio::buffer("delete success"), sendpoint);
			}
			else if (vtmp[0] == "upd" && vtmp.size() == 3){
				key = std::stol(vtmp[1]);
				split(vtmp[2], ",", vv);
				vl.init();
				for (i = 0; i < vv.size(); i++){
					vl.items[i] = std::stoi(vv[i]);
				}
				templatedb::CDB::getInstance().upd(key, vl);
				udp_socket.send_to(asio::buffer("update success"), sendpoint);
			}
			else if (vtmp[0] == "scan" && vtmp.size() == 2){
				split(vtmp[1], ",", vv);
				if (vv.size() == 2){
					key = std::stol(vv[0]);
					key_ = std::stol(vv[1]);
				}
				else{
					key = 1;
					key_ = -1;
				}
				templatedb::CDB::getInstance().scan(key, key_, vvl);
				tmp = "";
				for (i = 0; i < vvl.size(); i++){
					tmp += vvl[i].toStr() + "\n";
				}
				udp_socket.send_to(asio::buffer(tmp), sendpoint);
			}
			else{
				udp_socket.send_to(asio::buffer("parameter error"), sendpoint);
			}
		}
		else{
			udp_socket.send_to(asio::buffer("parameter error"), sendpoint);
		}
		memset(receive_str, 0, 1024);
	}


	mlogS::getInstance().write("serverRun exit");
}
void run(){
	lThread::getInstance().start();

	if (myPar::getInstance().option == "test"){
		mlogS::getInstance().write("run test");
		run_btest(&myPar::getInstance());
		mlogS::getInstance().write("run test done");
	}
	else{
		mlogS::getInstance().write("run server");
		myPar::getInstance().sthr = new std::thread(serverRun);
		myPar::getInstance().sthr->join();
		delete myPar::getInstance().sthr;
		mlogS::getInstance().write("run server done");
	}
}




bool checkMyselfExist()//如果程序已经有一个在运行，则返回true
{
#ifdef WIN32
	HANDLE  hMutex = CreateMutex(NULL, FALSE, L"DevState");
	if (hMutex && (GetLastError() == ERROR_ALREADY_EXISTS))
	{
		CloseHandle(hMutex);
		hMutex = NULL;
		return true;
	}
	else{
		return false;
	}
#else
	long pid;
	char full_name[FNSZ] = { 0 };
	char proc_name[FNSZ] = { 0 };
	int fd;
	pid = getpid();
	sprintf(full_name, "/proc/%ld/cmdline", pid);
	if (access(full_name, F_OK) == 0)
	{
		fd = open(full_name, O_RDONLY);
		if (fd == -1)
			return false;
		read(fd, proc_name, FNSZ);
		close(fd);
	}
	else
		return false;

	char self_proc_name[FNSZ] = { 0 };
	char * p = proc_name;
	int pt = 0;
	while (*p != ' ' && *p != '\0')
	{
		self_proc_name[pt] = *p;
		p++;
		pt++;
	}
	std::string self_final_name = basename(self_proc_name);
	DIR *dir;
	struct dirent * result;
	dir = opendir("/proc");
	while ((result = readdir(dir)) != NULL)
	{
		if (!strcmp(result->d_name, ".") || !strcmp(result->d_name, "..") || !strcmp(result->d_name, "thread-self")
			|| !strcmp(result->d_name, "self") || atol(result->d_name) == pid)
			continue;
		memset(full_name, 0, sizeof(full_name));
		memset(proc_name, 0, sizeof(proc_name));
		sprintf(full_name, "/proc/%s/cmdline", result->d_name);
		if (access(full_name, F_OK) == 0)
		{
			fd = open(full_name, O_RDONLY);
			if (fd == -1)
				continue;
			read(fd, proc_name, FNSZ);
			close(fd);
			char *q = proc_name;
			pt = 0;
			memset(self_proc_name, 0, sizeof (self_proc_name));
			while (*q != ' ' && *q != '\0')
			{
				self_proc_name[pt] = *q;
				q++;
				pt++;
			}
			std::string other_final_name = basename(self_proc_name);
			if (self_final_name == other_final_name)
			{
				return true;
			}
		}
	}
	return false;
#endif
}



int main(int argc, char* argv[])
{
	CountTime ct;
	ct.begin();
	try{

		mlogS::getInstance().write("checkMyselfExist");
		if (checkMyselfExist()){
			throw std::string("program has been running");
		}
		mlogS::getInstance().write("checkMyselfExist done");

		if (!getopt(argc, argv))
			return 1;

		init();	

		run();

		destroy();
	}
	catch (std::string e){
		mlogS::getInstance().write(e.c_str());
		printMe(e.c_str(), PEM);
	}
	catch (std::exception e){
		mlogS::getInstance().write(e.what());
		printMe(e.what(), PEM);
	}
	catch (...){
		mlogS::getInstance().write("Exception");
		printMe("Exception", PEM);
	}


	mlogS::getInstance().write("Success End");
	ct.oneEnd();
	ct.print("Process End");

	//system("pause");
	SYSPAUSE

	return 0;
}

