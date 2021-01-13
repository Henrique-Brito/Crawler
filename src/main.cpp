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
#include <ctime>

struct data{
	std::vector<std::pair<int, std::string>> url;
	data(){}
};

struct statistics{
	std::string website;
	int number;
	double average_size;
	double average_time;
	statistics(){};
	statistics(std::string s, int n, int as, double at){
		website = s;
		number = n;
		average_size = ((double)as)/n;
		average_time = ((double)at)/n;
	}
};

const int limit = 3000;

std::vector<std::vector<std::string>> new_links;

std::vector<int> qnt_crawled;
std::vector<int> size_html;
std::vector<double> time_crawling;

std::vector<statistics> to_output;

const bool debug = true;

void configure_spider( CkSpider& spider ){
	spider.put_Utf8(true);
	spider.put_ConnectTimeout(4);
	spider.put_ReadTimeout(16);
	spider.put_MaxResponseSize(2000000);
	spider.put_MaxUrlLen(150);
}

CkSpider sdr;
std::string domain(std::string& s){
	return sdr.getUrlDomain(s.c_str());
}


std::string collect_html(std::string url){
	CkSpider spider;
	configure_spider(spider);
	spider.Initialize(url.c_str());
	spider.CrawlNext();
	return spider.lastHtml();
}

const std::string file_prefix="data/website";
const std::string file_sufix=".html";

pthread_mutex_t mutex;

int total_crawled=0; 
int file_counter=0;

int output_html(std::string html){

	std::ofstream out(file_prefix+std::to_string(file_counter)+file_sufix);
	file_counter++;

	if( out.is_open() ){
		out << html;
	}else{
		std::cout << "Error: cant open write file - HTML" << '\n';
	}

	return (int)html.size();
}

bool crawl(int id, std::string url){

	CkSpider spider;
	configure_spider(spider);
	spider.Initialize(url.c_str());

	spider.SleepMs(100);
	spider.CrawlNext();
	output_html(spider.lastHtml());

	if( debug ) std::cout << "Initial string: " << url << '\n';

	bool leave=false;

	for( int i=0; i<spider.get_NumUnspidered(); i++ ){
		spider.SleepMs(100);

		pthread_mutex_lock(&mutex);

		clock_t begin = clock();

		size_html[id] += output_html(collect_html(std::string(spider.getUnspideredUrl(i))));

		clock_t end = clock();

		if( file_counter >= limit ){
			leave=true;
		}
		pthread_mutex_unlock(&mutex);

		time_crawling[id] += double(end-begin) / CLOCKS_PER_SEC;
		qnt_crawled[id]++;
		if( false ) std::cout << "Link " << i << ": " << spider.getUnspideredUrl(i) << '\n';
	}

	if( leave ) return true;

	for( int i=0; i<spider.get_NumOutboundLinks(); i++ ){
		new_links[id].push_back(std::string(spider.getOutboundLink(i)));
		if( false ) std::cout << "Link " << i << ": " << spider.getOutboundLink(i) << '\n';
	}

	return false;

}

void* short_term_scheduler(void* d){

	std::vector<std::pair<int, std::string>> url = ((data*)d)->url;  

	for( auto e : url ){
		if( crawl(e.first, e.second) ){
			pthread_exit(NULL);
		}	
	}

	pthread_exit(NULL);
}


void long_term_scheduler(std::vector<std::string>& url){

	int num_seeds=(int)url.size();

	std::map<std::string, std::vector<std::string>> host;

	for( std::string s : url ){
		std::string d(domain(s));
		host[d].push_back(s);
	}

	if( debug ) std::cout << "Host of initial urls:" << '\n';

	if( false ){
		for( auto e : host ){
			std::cout << e.first << ":" << '\n';
			for( auto s : e.second ){
				std::cout << s << '\n';
			}
		}
	}

	bool leave = false;
	while( not leave ){

		int n = (int)host.size();
		new_links.clear();
		new_links.resize(num_seeds);

		qnt_crawled.clear();
		qnt_crawled.resize(num_seeds, 0);

		size_html.clear();
		size_html.resize(num_seeds, 0);

		time_crawling.clear();
		time_crawling.resize(num_seeds, 0);

		std::vector<pthread_t> threads(n);
		
		if( debug ) std::cout << "Creating " << n << " Threads" << '\n';

		std::vector<std::string> websites;
		int i=0;
		int counter=0;
		for( auto e : host ){
			data* d = new data;

			for( std::string str : e.second ){
				d->url.push_back({counter++, str});
				websites.push_back(str);
			}

			int ret = pthread_create(&threads[i], NULL, short_term_scheduler, (void *)d);

			if( ret ){
				std::cout << "Error cant create thread" << '\n';
				exit(-1);
			}

			i++;
		}

		void *status;
		for( i=0; i<n; i++ ){
			int ret = pthread_join(threads[i], &status);
			if( ret ){
				std::cout << "Error cant join thread" << '\n';
				exit(-1);
			}
		}

		if( debug ) std::cout << "Joined all Threads" << '\n';

		for( int i=0; i<num_seeds; i++ ){
			to_output.push_back(statistics(websites[i], qnt_crawled[i], size_html[i], time_crawling[i]));
			total_crawled += qnt_crawled[i];
		}
		
		if( total_crawled >= limit ){
			leave = true;
			break;
		}

		host.clear();
		num_seeds=0;
		for( i=0; i<n; i++ ){
			num_seeds += new_links[i].size();
			for( std::string s : new_links[i] ){
				std::string d(domain(s));
				host[d].push_back(s);
			}
		}

		if( total_crawled >= limit ){
			leave = true;
		}
	}
}

int main( int argc, char** argv ){

	clock_t begin = clock();

	if( debug ) std::cout << "Begin" << '\n';
	
	if( argc < 2 ){
		std::cout << "Invalid parameters" << '\n';
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
		std::cout << "Error: cant open read file" << '\n';
		return 0;
	}

	if( debug ) std::cout << "Read input file" << '\n';

	pthread_mutex_init(&mutex, NULL);
	long_term_scheduler(url);

	if( debug ) std::cout << "Done Crawling" << '\n';

	std::ofstream out("out.txt");

	if( out.is_open() ){
		for( auto s : to_output ){
			out << s.website << std::endl;
			out << "Number at level 1 = " <<  s.number << '\n';
			out << "Average size = " <<  s.average_size << '\n';
			out << "Average time = " <<  s.average_time << '\n';
			out << "--------------------------------------------------------" << '\n';
		}
		out.close();
	}else{
		std::cout << "Error: cant open write file" << std::endl;
		return 0;
	}

	clock_t end = clock();

	double total_time_elapsed = double(end-begin) / CLOCKS_PER_SEC;
	std::cout << "Total time elapsed: " << total_time_elapsed << '\n';

}
