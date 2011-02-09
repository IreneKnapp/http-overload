#include <arpa/inet.h>
#include <assert.h>
#include <dispatch/dispatch.h>
#include <fcntl.h>
#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>


struct global_context {
    int n_connections_made;
    int n_requests_sent;
    int n_responses_received;
    struct statistics *statistics;
    struct parameters *parameters;
    int family;
    int socktype;
    int protocol;
    socklen_t sockaddr_size;
    struct sockaddr *sockaddr;
    uint64_t timeout;
    dispatch_source_t abort_source;
    struct context *final_context;
};


struct context {
    int fd;
    dispatch_source_t create_source;
    dispatch_source_t write_source;
    dispatch_source_t read_source;
    struct request *request;
    struct global_context *global_context;
};


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
    dispatch_time_t time_initiated;
    dispatch_time_t time_completed;
    dispatch_time_t total_duration;
};


struct request {
    bool request_sent;
    bool response_received;
    bool response_timed_out;
    int response_status;
    int response_length;
    dispatch_time_t time_initiated;
    dispatch_time_t time_completed;
    struct context context;
};


void report(struct statistics *statistics);
void prepare(struct parameters *parameters);
void prepare_network(struct global_context *global_context);
void prepare_heavy_lifting(struct global_context *global_context);
void measure(struct statistics *statistics, struct parameters *parameters);
void finish_measuring(struct context *context);
void initiate_connection(struct context *context);
void event(struct context *context);


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
    if(!have_path) {
	fprintf(stderr, "Must provide a path, with --path <path>.\n");
	missing_any = 1;
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
    
    prepare(parameters);
    dispatch_main();
    return 0;
    
usage:
    fprintf(stderr, "Usage: http-overload ");
    fprintf(stderr, "[--server <hostname>] [--port <port>] [--path <path>]\n");
    fprintf(stderr, "                     ");
    fprintf(stderr, "[--n-requests <n>] [--rate <n/s>] [--timeout <s>]\n");
    return 1;
}


void report(struct statistics *statistics) {
    printf("%i total requests sent over %f seconds.\n",
	   statistics->n_requests_sent,
	   ((float) statistics->total_duration) / 1.0e9);
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


void prepare(struct parameters *parameters) {
    struct global_context *global_context
	= malloc(sizeof(struct global_context));
    
    global_context->n_connections_made = 0;
    global_context->n_requests_sent = 0;
    global_context->n_responses_received = 0;
    global_context->statistics = NULL;
    global_context->parameters = parameters;
    global_context->timeout = ((uint64_t) parameters->timeout) * 1000000000;
    
    prepare_network(global_context);
    
    printf("Benching http://%s:%i%s will start in 100ms.\n",
	   parameters->server,
	   parameters->port,
	   parameters->path);
    fflush(stdout);
    
    prepare_heavy_lifting(global_context);
}


void prepare_network(struct global_context *global_context) {
    struct parameters *parameters = global_context->parameters;
    
    struct protoent *protoent = NULL;
    
    protoent = getprotobyname("IP");
    if(!protoent) {
	fprintf(stderr, "Error looking up IP protocol number.\n");
	exit(1);
    }
    int ip = protoent->p_proto;
    
    protoent = getprotobyname("TCP");
    if(!protoent) {
	fprintf(stderr, "Error looking up TCP protocol number.\n");
	exit(1);
    }
    int tcp = protoent->p_proto;
    
    endprotoent();
    
    char *hostname = parameters->server;
    char port[80];
    snprintf(port, 80, "%i", parameters->port);
    
    struct addrinfo hints;
    hints.ai_flags = AI_ADDRCONFIG
	             | AI_ALL
	             | AI_NUMERICSERV;
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_addrlen = .0;
    hints.ai_addr = NULL;
    hints.ai_canonname = NULL;
    hints.ai_next = NULL;
    
    struct addrinfo *addrinfos;
    if(getaddrinfo(hostname, port, &hints, &addrinfos)) {
	fprintf(stderr, "Error while attempting to resolve address.\n");
	exit(1);
    }
    if(!addrinfos) {
	fprintf(stderr, "Server not found.\n");
	exit(1);
    }
    
    int family = addrinfos->ai_family;
    int socktype = addrinfos->ai_socktype;
    int protocol = addrinfos->ai_protocol;
    socklen_t sockaddr_size = addrinfos->ai_addrlen;
    struct sockaddr *sockaddr = malloc(sockaddr_size);
    memcpy(sockaddr, addrinfos->ai_addr, sockaddr_size);
    
    freeaddrinfo(addrinfos);
    
    global_context->family = family;
    global_context->socktype = socktype;
    global_context->protocol = protocol;
    global_context->sockaddr_size = sockaddr_size;
    global_context->sockaddr = sockaddr;
}


void prepare_heavy_lifting(struct global_context *global_context) {
    struct parameters *parameters = global_context->parameters;
    
    dispatch_time_t time_zero = dispatch_time(DISPATCH_TIME_NOW, 100000);
    
    dispatch_queue_t queue = dispatch_get_main_queue();
    
    int64_t interval_between_requests
	= (int64_t) (((float) 1000000000) / ((float) parameters->rate));
    
    struct statistics *statistics = malloc(sizeof(struct statistics));
    struct context *final_context = malloc(sizeof(struct context));
    
    statistics->n_requests = parameters->n_requests;
    statistics->requests
	= malloc(statistics->n_requests * sizeof(struct request));
    for(int i = 0; i < statistics->n_requests; i++) {
	struct request *request = &statistics->requests[i];
	
	dispatch_source_t source
	    = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER,
				     0,
				     0,
				     queue);
	dispatch_set_context(source, &request->context);
	dispatch_source_set_event_handler_f(source, (void (*)(void *)) event);
	dispatch_source_set_timer(source,
				  time_zero,
				  i * interval_between_requests,
				  0);
	
	request->request_sent = 0;
	request->response_received = 0;
	request->response_timed_out = 0;
	request->context.fd = -1;
	request->context.create_source = NULL;
	request->context.read_source = NULL;
	request->context.write_source = NULL;
	request->context.request = request;
	request->context.global_context = global_context;
	
	dispatch_resume(source);
    }
    
    final_context->fd = -1;
    final_context->create_source = NULL;
    final_context->read_source = NULL;
    final_context->write_source = NULL;
    final_context->request = NULL;
    final_context->global_context = global_context;
    
    global_context->final_context = final_context;
    
    statistics->statistics_valid = 0;
    statistics->time_initiated = time_zero;
    
    global_context->statistics = statistics;

    signal(SIGINT, SIG_IGN);

    dispatch_source_t abort_source
	= dispatch_source_create(DISPATCH_SOURCE_TYPE_SIGNAL,
				 SIGINT,
				 0,
				 queue);
    dispatch_set_context(abort_source, final_context);
    dispatch_source_set_event_handler_f(abort_source,
					(void (*)(void *)) finish_measuring);
    dispatch_resume(abort_source);
    
    global_context->abort_source = abort_source;
}


void finish_measuring(struct context *context) {
    struct statistics *statistics = context->global_context->statistics;
    
    statistics->time_completed = dispatch_time(DISPATCH_TIME_NOW, 0);
    
    statistics->total_duration
	= statistics->time_completed - statistics->time_initiated;
    int n_requests_sent = 0;
    int n_responses = 0;
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
    
    report(statistics);
    exit(0);
}


void initiate_connection(struct context *context) {
    struct global_context *global_context = context->global_context;
    
    int fd = socket(global_context->family,
		    global_context->socktype,
		    global_context->protocol);
    if(fd == -1) {
	fprintf(stderr, "Error creating socket.\n");
	exit(1);
    }
    fcntl(fd, F_SETFL, O_NONBLOCK);
    connect(fd, global_context->sockaddr, global_context->sockaddr_size);
    
    context->fd = fd;
    
    dispatch_queue_t queue = dispatch_get_main_queue();
    
    dispatch_source_t source
	= dispatch_source_create(DISPATCH_SOURCE_TYPE_WRITE,
				 context->fd,
				 0,
				 queue);
    context->write_source = source;
    dispatch_set_context(source,
			 (void *) context);
    dispatch_source_set_event_handler_f(source,
					(void (*)(void *)) event);
    dispatch_source_set_timer(source,
			      DISPATCH_TIME_NOW,
			      global_context->timeout,
			      0);
    dispatch_resume(source);
}


void event(struct context *context) {
    
}
