#include <iostream>
#include <CkSpider.h>
#include <stdlib.h>
#include <ctime>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <unistd.h>
#include <fstream>

struct data{
	int id;
	std::vector<std::pair<int, std::string>> url;
	data(){}
};

const int limit = 1000;

std::vector<std::string> all_links;
std::vector<std::set<std::string>> new_links;

const bool debug = false;

CkSpider sdr;
std::string domain(std::string& s){
	return sdr.getBaseDomain(s.c_str());
}

std::string collect_html(std::string url){
	CkSpider spider;
	spider.Initialize(url.c_str());
	spider.CrawlNext();
	return spider.lastHtml();
}

const std::string file_prefix="data/website";
const std::string file_sufix=".html";

int file_counter=0;
void output_html(std::string html){
	
	std::ofstream out(file_prefix+std::to_string(file_counter)+file_sufix);
	file_counter++;

	if( out.is_open() ){
		out << html;
	}else{
		std::cout << "Error: cant open write file - HTML" << std::endl;
	}
}

void crawl(int id, std::string url){

	CkSpider spider;
	spider.Initialize(url.c_str());

	spider.CrawlNext();
	output_html(spider.lastHtml());

	if( debug ) std::cout << "Initial string: " << url << std::endl;

	for( int i=0; i<spider.get_NumUnspidered(); i++ ){
		output_html(std::string(spider.getUnspideredUrl(i)));
		if( false ) std::cout << "Link " << i << ": " << spider.getUnspideredUrl(i) << std::endl;
	}

	for( int i=0; i<spider.get_NumOutboundLinks(); i++ ){
		new_links[id].insert(std::string(spider.getOutboundLink(i)));
		if( false ) std::cout << "Link " << i << ": " << spider.getOutboundLink(i) << std::endl;
	}


}

void* short_term_scheduler(void* d){

	std::vector<std::pair<int, std::string>> url = ((data*)d)->url;  

	for( auto e : url ){
		crawl(e.first, e.second);
		usleep(100000);
	}

	pthread_exit(NULL);
}

void long_term_scheduler(std::vector<std::string>& url){
	
	std::map<std::string, std::vector<std::string>> host;

	for( std::string s : url ){
		all_links.push_back(s);
		std::string d(domain(s));
		host[d].push_back(s);
	}

	if( debug ) std::cout << "Host of initial urls:" << std::endl;

	if( debug ){
		for( auto e : host ){
			std::cout << e.first << ":" << std::endl;
			for( auto s : e.second ){
				std::cout << s << std::endl;
			}
		}
	}

	bool leave = false;
	while( not leave ){

		int n = (int)host.size();
		new_links.clear();
		new_links.resize(n);
		
		std::vector<pthread_t> threads(n);

		int i=0;
		for( auto e : host ){
			data* d = new data;
			d->url = e.second;
			d->id = i;
			int ret = pthread_create(&threads[i], NULL, short_term_scheduler, (void *)d);

			if( ret ){
				std::cout << "Error cant create thread" << std::endl;
				exit(-1);
			}

			i++;
		}

		void *status;
		for( i=0; i<n; i++ ){
			int ret = pthread_join(threads[i], &status);
			if( ret ){
				std::cout << "Error cant join thread" << std::endl;
				exit(-1);
			}
		}

		if( debug ) std::cout << "Joined all Threads" << std::endl;

		host.clear();
		for( i=0; i<n and not leave; i++ ){
			for( std::string s : new_links[i] ){
				all_links.push_back(s);
				if( all_links.size() >= limit ){
					leave = true;
					break;
				}
				std::string d(domain(s));
				host[d].push_back(s);
			}
		}
	}
}

int main( int argc, char** argv ){

	if( argc < 2 ){
		std::cout << "Invalid parameters" << std::endl;
		return 0;
	}

	std::ifstream in(argv[1]);

	std::vector<std::string> url;

	if( in.is_open() ){
		std::string line;
		while( std::getline(in, line) ){
			url.push_back(line);
		}
		in.close();
	}else{
		std::cout << "Error: cant open read file" << std::endl;
		return 0;
	}

	if( debug ) std::cout << "Read input file" << std::endl;

	long_term_scheduler(url);

	if( debug ) std::cout << "Done Crawling" << std::endl;

	std::ofstream out("out.txt");

	if( out.is_open() ){
		for( auto s : all_links ){
			out << s << std::endl;
		}
		out.close();
	}else{
		std::cout << "Error: cant open write file" << std::endl;
		return 0;
	}


}
