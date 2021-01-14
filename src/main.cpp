#include <iostream>
#include <CkSpider.h>
#include <stdlib.h>
#include <ctime>
#include <string>
#include <vector>
#include <map>
#include <queue>
#include <set>
#include <unistd.h>
#include <fstream>
#include <ctime>

#define NUM_THREADS 12

const bool debug = true;
const int limit = 50000;
const int limit_level_1 = 300;
const int limit_outbound_links = 300;
const int limit_queue_size = 50000;
const std::string file_prefix="data/website";
const std::string file_sufix=".html";

struct data{
	int id;
	data(){}
};

struct statistics{
	std::string website;
	int number;
	double average_size;
	double average_time;
	statistics(){};
	statistics(std::string s){
		website = s;
		number = 0;
		average_size = 0.0;
		average_time = 0.0;
	}
	void process(){
		average_size /= number;
		average_time /= number;
	}
};

struct seed_queue{

	int counter;
	std::queue< std::vector<std::pair<int, std::string>> > q;
	pthread_mutex_t mutex_queue;
	std::vector<statistics> to_output;

	seed_queue(){
		counter=0;
		pthread_mutex_init(&mutex_queue, NULL);
	}

	void add_task(std::vector<std::string>& v){
		pthread_mutex_lock(&mutex_queue);

		if( q.size() <= limit_queue_size ){
			std::vector<std::pair<int, std::string>> to_add;
			for( std::string s : v ){
				to_add.push_back({counter, s});
				counter++;
				to_output.push_back(statistics(s));
			}
			q.push(to_add);
		}

		pthread_mutex_unlock(&mutex_queue);
	}

	std::vector<std::pair<int, std::string>> get_task(){
		std::vector<std::pair<int, std::string>> ret; 	

		pthread_mutex_lock(&mutex_queue);

		if( q.size() == 0 ){
			pthread_mutex_unlock(&mutex_queue);
			return ret;
		}
		ret = q.front();
		q.pop();

		pthread_mutex_unlock(&mutex_queue);

		return ret;
	}

};

struct new_links_queue{

	std::queue<std::string> q;
	pthread_mutex_t mutex_queue;

	new_links_queue(){
		pthread_mutex_init(&mutex_queue, NULL);
	}

	void add_link(std::vector<std::string>& v){
		pthread_mutex_lock(&mutex_queue);

		if( q.size() <= limit_queue_size ){
			for( std::string s : v ){
				q.push(s);
			}
		}
		pthread_mutex_unlock(&mutex_queue);
	}

	std::vector<std::string> get_link(){
		std::vector<std::string> ret;
		pthread_mutex_lock(&mutex_queue);
		while( q.size() ){
			ret.push_back(q.front());
			q.pop();
		}
		pthread_mutex_unlock(&mutex_queue);
		return ret;
	}

};

seed_queue Q = seed_queue();
new_links_queue link_Q = new_links_queue();

pthread_t threads[NUM_THREADS];

pthread_mutex_t mutex_file_output, mutex_counter;

int url_counter=0;
int file_counter=0;

int total_crawled(){
	pthread_mutex_lock(&mutex_counter);
	int ret = url_counter;
	pthread_mutex_unlock(&mutex_counter);
	return ret;
}

void add_crawled(int val){
	pthread_mutex_lock(&mutex_counter);
	url_counter += val;
	pthread_mutex_unlock(&mutex_counter);
}

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

bool collect_html(std::string& url, std::string& ret){

	CkSpider spider;
	configure_spider(spider);
	spider.Initialize(url.c_str());

	bool ok = spider.CrawlNext();
	if( not ok ) return false;
	ret = spider.lastHtml();

	return true;
}

int output_html(std::string html){

	pthread_mutex_lock(&mutex_file_output);

	std::ofstream out(file_prefix+std::to_string(file_counter)+file_sufix);
	file_counter++;

	if( out.is_open() ){
		out << html;
	}else{
		std::cout << "Error: cant open write file - HTML" << '\n';
	}

	pthread_mutex_unlock(&mutex_file_output);

	return (int)html.size();
}

void crawl( int id, std::string url ){

	CkSpider spider;
	configure_spider(spider);
	spider.Initialize(url.c_str());

	spider.SleepMs(100);

	bool ok = spider.CrawlNext();

	if( not ok ){	
		return;
	}

	output_html(spider.lastHtml());

	int qnt=0;

	for( int i=0; i<spider.get_NumUnspidered() and i<limit_level_1; i++ ){

		if( i%10 == 0 and total_crawled() > limit ) break;

		spider.SleepMs(100);

		clock_t begin = clock();

		std::string url(spider.getUnspideredUrl(i)); 

		std::string html;
		bool ok = collect_html(url, html);

		if( ok ){
			output_html(html);
		}

		clock_t end = clock();

		if( ok ){
			Q.to_output[id].average_time += double(end-begin) / CLOCKS_PER_SEC;
			Q.to_output[id].number++; qnt++;
			Q.to_output[id].average_size += (int)html.size();
		}
	}

	add_crawled(qnt+1);

	std::vector<std::string> links;
	for( int i=0; i<spider.get_NumOutboundLinks() and i<limit_outbound_links; i++ ){
		links.push_back(std::string(spider.getOutboundLink(i)));
	}

	link_Q.add_link(links);

}

void* short_term_scheduler(void* arg){

	//int id = ((data*)arg)->id;
	while( total_crawled() <= limit ){

		//if( debug and id == 0 ) std::cout << "Here"<< std::endl;

		auto v = Q.get_task();

		if( v.size() == 0 ){
			usleep(300000);
			continue;
		}

		for( auto e : v ){
			crawl(e.first, e.second);
			if( total_crawled() > limit ){
				break;
			}
		}
	}
	pthread_exit(NULL);
}

void group_by_host( std::map<std::string, std::vector<std::string>>& host, std::vector<std::string>& urls ){
	host.clear();
	for( std::string s : urls ){
		std::string d(domain(s));
		host[d].push_back(s);
	}
}

void long_term_scheduler(std::vector<std::string>& seed_url){

	std::map<std::string, std::vector<std::string>> host;

	group_by_host(host, seed_url);

	for( auto e : host ){
		Q.add_task(e.second);
	}	

	if( debug ) std::cout << "Added initial urls" << std::endl;

	while( total_crawled() <= limit ){

		if( false ) std::cout << "In while loop" << std::endl;

		std::vector<std::string> links = link_Q.get_link();

		if( links.size() == 0 ){
			usleep(300000);
			continue;
		}

		group_by_host(host, links);

		for( auto e : host ){
			Q.add_task(e.second);
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

	pthread_mutex_init(&mutex_file_output, NULL);
	pthread_mutex_init(&mutex_counter, NULL);

	for( int i=0; i<NUM_THREADS; i++ ){
		data* d = new data;
		d->id = i;
		int ret = pthread_create(&threads[i], NULL, short_term_scheduler, (void*)d);

		if( ret ){
			std::cout << "Error cant create thread" << '\n';
			exit(-1);
		}
	}

	if( debug ) std::cout << "Created all threads" << '\n';

	long_term_scheduler(url);

	void* status;
	for( int i=0; i<NUM_THREADS; i++ ){
		int ret = pthread_join(threads[i], &status);
		if( ret ){
			std::cout << "Error cant join thread" << '\n';
			exit(-1);
		}
	}

	if( debug ) std::cout << "Done Crawling" << '\n';

	std::ofstream out("out.txt");

	if( out.is_open() ){
		for( auto s : Q.to_output ){
			if( s.number == 0 ) continue;
			s.process();
			out << s.website << std::endl;
			out << "Number of websites at level 1 = " <<  s.number << '\n';
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
