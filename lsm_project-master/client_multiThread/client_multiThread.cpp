// client_multiThread.cpp : �������̨Ӧ�ó������ڵ㡣
//

#pragma warning(disable:4996)

#include "asio.hpp"
#include <iostream>

int main(void)
{
	std::cout << "Usage: \n\t1.get:key\n\t2.put:key:v1,v2,v3,v4\n\t3.del:key\n\t4.upd:key:v1,v2,v3,v4\n\t5.scan:min,max\n";
	std::cout << "Examples:\n\tput:1:1,2,3,4\n\tget:1\n";
	std::cout << "Input:\n";

	asio::io_service io_service;
	asio::ip::udp::socket udp_socket(io_service);
	asio::ip::udp::endpoint local_add(asio::ip::address::from_string("127.0.0.1"), 2000);
	udp_socket.open(local_add.protocol());

	char receive_str[1024] = { 0 };//�ַ���
	while (1)
	{
		asio::ip::udp::endpoint  sendpoint;//�����IP�Լ��˿�
		std::string s;
		std::cin >> s;
		udp_socket.send_to(asio::buffer(s.c_str(), s.size()), local_add);
		udp_socket.receive_from(asio::buffer(receive_str, 1024), local_add);//��ȡ
		std::cout << "Receive : \n===================\n " << receive_str << "\n===================" << std::endl;
		memset(receive_str, 0, 1024);//����ַ���
	}
	return 0;
}