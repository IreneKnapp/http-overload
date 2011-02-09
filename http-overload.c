#include <assert.h>
#include <dispatch/dispatch.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <time.h>


struct parameters {
    char *server;
    int port;
    char *path;
    int n_requests;
    int rate;
    int timeout;
};


struct statistics {
    int n_requests;
    struct request *requests;
    bool statistics_valid;
    int n_requests_sent;
    int n_successes;
    int n_errors;
    int n_timeouts;
    float min_success_time;
    float max_success_time;
    float average_success_time;
    float min_error_time;
    float max_error_time;
    float average_error_time;
    float min_response_length;
    float max_response_length;
    float average_response_length;
    time_t time_initiated;
    time_t time_completed;
    time_t total_duration;
};


struct request {
    bool request_sent;
    bool response_received;
    bool response_timed_out;
    int response_status;
    int response_length;
    time_t time_initiated;
    time_t time_completed;
};


void prepare(struct statistics **statistics_out,
	     struct parameters *parameters);
void measure(struct statistics *statistics, struct parameters *parameters);
void report(struct statistics *statistics);


int main(int argc, char **argv) {
    bool have_server = 0;
    bool have_port = 0;
    bool have_path = 0;
    bool have_n_requests = 0;
    bool have_rate = 0;
    bool have_timeout = 0;
    struct parameters *parameters;
    parameters = malloc(sizeof(struct parameters));
    
    for(int i = 1; i < argc; i++) {
	if(!strcmp(argv[i], "--server")) {
	    if(i + 1 < argc) {
		i++;
		parameters->server = argv[i];
		have_server = 1;
	    } else goto usage;
	} else if(!strcmp(argv[i], "--port")) {
	    if(i + 1 < argc) {
		i++;
		parameters->port = atoi(argv[i]);
		have_port = 1;
	    } else goto usage;
	} else if(!strcmp(argv[i], "--path")) {
	    if(i + 1 < argc) {
		i++;
		parameters->path = argv[i];
		have_path = 1;
	    } else goto usage;
	} else if(!strcmp(argv[i], "--n-requests")) {
	    if(i + 1 < argc) {
		i++;
		parameters->n_requests = atoi(argv[i]);
		have_n_requests = 1;
	    } else goto usage;
	} else if(!strcmp(argv[i], "--rate")) {
	    if(i + 1 < argc) {
		i++;
		parameters->rate = atoi(argv[i]);
		have_rate = 1;
	    } else goto usage;
	} else if(!strcmp(argv[i], "--timeout")) {
	    if(i + 1 < argc) {
		i++;
		parameters->timeout = atoi(argv[i]);
		have_timeout = 1;
	    } else goto usage;
	} else goto usage;
    }
    
    bool missing_any = 0;
    if(!have_server) {
	fprintf(stderr, "Must provide a server, with --server <hostname>.\n");
	missing_any = 1;
    }
    if(!have_port) {
	parameters->port = 80;
    }
    if(!have_n_requests) {
	fprintf(stderr,
		"Must provide number of connections, with --n-requests <n>.\n");
	missing_any = 1;
    }
    if(!have_rate) {
	fprintf(stderr, "Must provide connection rate, with --rate <n/s>.\n");
	missing_any = 1;
    }
    if(!have_timeout) {
	fprintf(stderr, "Must provide timeout, with --timeout <s>.\n");
	missing_any = 1;
    }
    if(missing_any) {
	return 1;
    }
    
    struct statistics *statistics = NULL;
    prepare(&statistics, parameters);
    measure(statistics, parameters);
    report(statistics);
    return 0;
    
usage:
    fprintf(stderr, "Usage: http-overload ");
    fprintf(stderr, "[--server <hostname>] [--port <port>] [--path <path>]\n");
    fprintf(stderr, "                     ");
    fprintf(stderr, "[--n-requests <n>] [--rate <n/s>] [--timeout <s>]\n");
    return 1;
}


void prepare(struct statistics **statistics_out,
	     struct parameters *parameters)
{
    struct statistics *statistics = malloc(sizeof(struct statistics));
    statistics->n_requests = parameters->n_requests;
    statistics->requests
	= malloc(statistics->n_requests * sizeof(struct request));
    for(int i = 0; i < statistics->n_requests; i++) {
	struct request *request = &statistics->requests[i];
	request->request_sent = 0;
	request->response_received = 0;
	request->response_timed_out = 0;
    }
    statistics->statistics_valid = 0;
    
    *statistics_out = statistics;
}


void report(struct statistics *statistics) {
    if(!statistics->statistics_valid) {
	int n_requests_sent = 0;
	int n_successes = 0;
	int n_errors = 0;
	int n_timeouts = 0;
	float min_success_time = INFINITY;
	float max_success_time = -INFINITY;
	float total_success_time = 0;
	float min_error_time = INFINITY;
	float max_error_time = -INFINITY;
	float total_error_time = 0;
	float min_response_length = INFINITY;
	float max_response_length = -INFINITY;
	float total_response_length = 0;
	
	for(int i = 0; i < statistics->n_requests; i++) {
	    struct request *request = &statistics->requests[i];
	    
	    if(request->request_sent) {
		n_requests_sent++;
		if(request->response_received) {
		    float time = (float) (request->time_completed
					  - request->time_initiated);
		    if(request->response_status / 100 == 2) {
			n_successes++;
			if(time < min_success_time) min_success_time = time;
			if(time > max_success_time) max_success_time = time;
			total_success_time += time;

			float response_length
			    = (float) request->response_length;
			if(response_length < min_response_length)
			    min_response_length = response_length;
			if(response_length > max_response_length)
			    max_response_length = response_length;
			total_response_length += response_length;
		    } else {
			n_errors++;
			if(time < min_error_time) min_error_time = time;
			if(time > max_error_time) max_error_time = time;
			total_error_time += time;
		    }
		} else if(request->response_timed_out) {
		    n_timeouts++;
		} else assert(0);
	    }
	}
	
	statistics->n_requests_sent = n_requests_sent;
	statistics->n_successes = n_successes;
	statistics->n_errors = n_errors;
	statistics->n_timeouts = n_timeouts;
	
	statistics->min_success_time = min_success_time;
	statistics->max_success_time = max_success_time;
	statistics->average_success_time
	    = total_success_time / (float) n_successes;
	
	statistics->min_error_time = min_error_time;
	statistics->max_error_time = max_error_time;
	statistics->average_error_time
	    = total_error_time / (float) n_errors;
	
	statistics->min_response_length = min_response_length;
	statistics->max_response_length = max_response_length;
	statistics->average_response_length
	    = total_response_length / (float) n_successes;
	
	statistics->total_duration =
	    statistics->time_completed - statistics->time_initiated;
	
	statistics->statistics_valid = 1;
    }
    
    printf("%i total requests sent over %li seconds.\n",
	   statistics->n_requests_sent,
	   (long) statistics->total_duration);
    printf("\n");
    printf("%i total successes.\n",
	   statistics->n_successes);
    printf("Success times (min/avg/max, per-second): %f/%f/%f, %f\n",
	   statistics->min_success_time,
	   statistics->average_success_time,
	   statistics->max_success_time,
	   1.0 / statistics->average_success_time);
    printf("Response lengths (min/avg/max): %f/%f/%f\n",
	   statistics->min_response_length,
	   statistics->average_response_length,
	   statistics->max_response_length);
    printf("\n");
    printf("%i total errors.\n",
	   statistics->n_errors);
    printf("Error times (min/avg/max, per-second): %f/%f/%f, %f\n",
	   statistics->min_error_time,
	   statistics->average_error_time,
	   statistics->max_error_time,
	   1.0 / statistics->average_error_time);
    printf("\n");
    printf("%i total timeouts.\n",
	   statistics->n_timeouts);
    printf("\n");
    printf("Too slow - make it faster!\n");
}


void measure(struct statistics *statistics, struct parameters *parameters) {
}
