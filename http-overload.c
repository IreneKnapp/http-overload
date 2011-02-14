#include <arpa/inet.h>
#include <assert.h>
#include <dispatch/dispatch.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>


struct global_context {
    int n_connections_made;
    int n_remaining_connections;
    int n_requests_sent;
    int n_responses_received;
    int n_request_contexts;
    struct context *request_contexts;
    struct statistics *statistics;
    struct parameters *parameters;
    int family;
    int socktype;
    int protocol;
    int ip;
    int tcp;
    socklen_t sockaddr_size;
    struct sockaddr *sockaddr;
    uint64_t timeout;
    dispatch_source_t abort_source;
    int highest_good_fd;
    char *request_text;
    size_t request_text_length;
};


struct context {
    int fd;
    char *request_text_unsent;
    size_t request_text_unsent_size;
    dispatch_source_t initiate_source;
    dispatch_source_t create_source;
    dispatch_source_t write_source;
    dispatch_source_t read_source;
    struct global_context *global_context;
    bool connection_initiated;
    bool request_sent;
    bool request_failed;
    bool response_disconnected;
    bool response_received;
    bool response_timed_out;
    int response_status;
    int response_length;
    dispatch_time_t request_time_initiated;
    dispatch_time_t response_time_completed;
    size_t response_buffer_size;
    size_t response_buffer_length;
    uint8_t *response_buffer;
    uint8_t *response_buffer_unused_portion;
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
    bool statistics_valid;
    int n_requests_sent;
    int n_successes;
    int n_errors;
    int n_timeouts;
    int n_disconnects;
    int n_failures_to_connect;
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


void report(struct statistics *statistics);
void prepare(struct parameters *parameters);
void prepare_rlimit(struct global_context *global_context);
void prepare_network(struct global_context *global_context);
void prepare_request_text(struct global_context *global_context);
void prepare_heavy_lifting(struct global_context *global_context);
void measure(struct statistics *statistics, struct parameters *parameters);
void finish_measuring(struct global_context *global_context);
void initiate_connection(struct context *context);
void terminate_connection(struct context *context);
int handle_timeout_and_errors(struct context *context);
void write_event(struct context *context);
void read_event(struct context *context);


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
    printf("%i failures to connect, %i disconnects, and %i timeouts.\n",
           statistics->n_failures_to_connect,
           statistics->n_disconnects,
           statistics->n_timeouts);
    printf("\n");
    printf("Too slow - make it faster!\n");
}


void prepare(struct parameters *parameters) {
    struct global_context *global_context
        = malloc(sizeof(struct global_context));
    
    global_context->n_connections_made = 0;
    global_context->n_remaining_connections = parameters->n_requests;
    global_context->n_requests_sent = 0;
    global_context->n_responses_received = 0;
    global_context->statistics = NULL;
    global_context->parameters = parameters;
    global_context->timeout = ((uint64_t) parameters->timeout) * 1000000000;
    global_context->n_request_contexts = parameters->n_requests;
    
    prepare_rlimit(global_context);
    
    prepare_network(global_context);
    
    prepare_request_text(global_context);
    
    printf("Benching http://%s:%i%s will start in 100ms.\n",
           parameters->server,
           parameters->port,
           parameters->path);
    fflush(stdout);
    
    prepare_heavy_lifting(global_context);
}


void prepare_rlimit(struct global_context *global_context) {
    struct parameters *parameters = global_context->parameters;

    int next_fd = dup(0);
    if(next_fd == -1) {
        fprintf(stderr,
                "Error trying to predict how many fds will be needed.\n");
        exit(1);
    }
    close(next_fd);
    
    global_context->highest_good_fd = next_fd;
    
    // Grand Central Dispatch creates an fd when we first set a timer, and
    // keeps it around.  So we need to allow for it.
    rlim_t n_fds_needed = next_fd + global_context->n_request_contexts + 1;
    
    struct rlimit rlimit;
    getrlimit(RLIMIT_NOFILE, &rlimit);
    if(rlimit.rlim_cur < n_fds_needed) {
        if(rlimit.rlim_max < n_fds_needed) {
            rlimit.rlim_cur = n_fds_needed;
            rlimit.rlim_max = n_fds_needed;
        } else {
            rlimit.rlim_cur = n_fds_needed;
        }
        
        if(-1 == setrlimit(RLIMIT_NOFILE, &rlimit)) {
            fprintf(stderr, "Unable to raise rlimit on fds (error %i).\n",
                    errno);
            exit(1);
        }
    }
    getrlimit(RLIMIT_NOFILE, &rlimit);
    if(rlimit.rlim_cur < n_fds_needed) {
        fprintf(stderr,
                "Unable to raise rlimit on fds "
                "(the system only gave us %lli).\n",
                (long long) rlimit.rlim_cur);
        exit(1);
    }
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
    global_context->ip = ip;
    global_context->tcp = tcp;
    global_context->sockaddr_size = sockaddr_size;
    global_context->sockaddr = sockaddr;
}


void prepare_request_text(struct global_context *global_context) {
    struct parameters *parameters = global_context->parameters;

    global_context->request_text_length
        = asprintf(&global_context->request_text,
                   "GET %s HTTP/1.1\r\nHost: %s\r\n\r\n",
                   parameters->path,
                   parameters->server);
}


void prepare_heavy_lifting(struct global_context *global_context) {
    struct parameters *parameters = global_context->parameters;
    
    dispatch_time_t time_zero = dispatch_time(DISPATCH_TIME_NOW, 100000);
    
    dispatch_queue_t queue = dispatch_get_main_queue();
    
    int64_t interval_between_requests
        = (int64_t) (((float) 1000000000) / ((float) parameters->rate));
    
    struct statistics *statistics = malloc(sizeof(struct statistics));
    
    global_context->request_contexts
        = malloc(global_context->n_request_contexts * sizeof(struct context));
    for(int i = 0; i < global_context->n_request_contexts; i++) {
        struct context *context = &global_context->request_contexts[i];
        
        dispatch_source_t initiate_source
            = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER,
                                     0,
                                     0,
                                     queue);
        dispatch_set_context(initiate_source, context);
        dispatch_source_set_event_handler_f
            (initiate_source, (void (*)(void *)) initiate_connection);
        dispatch_source_set_timer(initiate_source,
                                  time_zero,
                                  i * interval_between_requests,
                                  0);

        context->connection_initiated = 0;
        context->request_sent = 0;
        context->request_failed = 0;
        context->response_disconnected = 0;
        context->response_received = 0;
        context->response_timed_out = 0;
        context->fd = -1;
        context->initiate_source = initiate_source;
        context->create_source = NULL;
        context->read_source = NULL;
        context->write_source = NULL;
        context->global_context = global_context;
        context->request_text_unsent = global_context->request_text;
        context->request_text_unsent_size = global_context->request_text_length;
        
        context->response_buffer_size = 1024*1024;
        context->response_buffer = malloc(context->response_buffer_size);
        context->response_buffer_length = 0;
        context->response_buffer_unused_portion = context->response_buffer;
        
        dispatch_resume(initiate_source);
    }
    
    statistics->statistics_valid = 0;
    statistics->time_initiated = time_zero;
    
    global_context->statistics = statistics;
    
    signal(SIGINT, SIG_IGN);
    
    dispatch_source_t abort_source
        = dispatch_source_create(DISPATCH_SOURCE_TYPE_SIGNAL,
                                 SIGINT,
                                 0,
                                 queue);
    dispatch_set_context(abort_source, global_context);
    dispatch_source_set_event_handler_f(abort_source,
                                        (void (*)(void *)) finish_measuring);
    dispatch_resume(abort_source);
    
    global_context->abort_source = abort_source;
}


void finish_measuring(struct global_context *global_context) {
    struct statistics *statistics = global_context->statistics;
    
    statistics->time_completed = dispatch_time(DISPATCH_TIME_NOW, 0);
    
    statistics->total_duration
        = statistics->time_completed - statistics->time_initiated;
    int n_requests_sent = 0;
    int n_responses = 0;
    int n_successes = 0;
    int n_errors = 0;
    int n_timeouts = 0;
    int n_disconnects = 0;
    int n_failures_to_connect = 0;
    float min_success_time = INFINITY;
    float max_success_time = -INFINITY;
    float total_success_time = 0;
    float min_error_time = INFINITY;
    float max_error_time = -INFINITY;
    float total_error_time = 0;
    float min_response_length = INFINITY;
    float max_response_length = -INFINITY;
    float total_response_length = 0;
    
    for(int i = 0; i < global_context->n_request_contexts; i++) {
        struct context *context = &global_context->request_contexts[i];
        
        if(context->request_sent) {
            n_requests_sent++;
            
            if(context->request_failed) {
                n_disconnects++;
            } else if(context->response_received) {
                float time = (float) (context->response_time_completed
                                      - context->request_time_initiated);
                if(context->response_status / 100 == 2) {
                    n_successes++;
                    if(time < min_success_time) min_success_time = time;
                    if(time > max_success_time) max_success_time = time;
                    total_success_time += time;
                    
                    float response_length
                        = (float) context->response_length;
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
            } else if(context->response_timed_out) {
                n_timeouts++;
            } else assert(0);
        } else if(context->response_disconnected) {
            n_disconnects++;
        } else if(context->request_failed) {
            n_failures_to_connect++;
        } else {
            n_failures_to_connect++;
        }
    }
    
    statistics->n_requests_sent = n_requests_sent;
    statistics->n_successes = n_successes;
    statistics->n_errors = n_errors;
    statistics->n_timeouts = n_timeouts;
    statistics->n_disconnects = n_disconnects;
    statistics->n_failures_to_connect = n_failures_to_connect;
    
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
    context->request_time_initiated = dispatch_time(DISPATCH_TIME_NOW, 0);
    context->connection_initiated = 1;
    
    dispatch_release(context->initiate_source);
    context->initiate_source = NULL;
    
    struct global_context *global_context = context->global_context;
    
    int fd = socket(global_context->family,
                    global_context->socktype,
                    global_context->protocol);
    if(fd == -1) {
        if(errno == EMFILE) {
            fprintf(stderr,
                    "Out of file descriptors; highest okay one was %i.\n",
                    global_context->highest_good_fd);
        } else {
            fprintf(stderr, "Error creating socket: %i.\n",
                    errno);
        }
        exit(1);
    }
    
    if(fd > global_context->highest_good_fd)
        global_context->highest_good_fd = fd;
    
    int nodelay = 1;
    setsockopt(fd, global_context->tcp, TCP_NODELAY, &nodelay, sizeof(nodelay));
    
    fcntl(fd, F_SETFL, O_NONBLOCK);
    
    int connect_result
        = connect(fd, global_context->sockaddr, global_context->sockaddr_size);
    if((-1 == connect_result) && (errno != EINPROGRESS)) {
        context->request_failed = 1;
        
        close(fd);
        context->fd = -1;
        
        terminate_connection(context);
        return;
    }
    
    context->fd = fd;
    
    dispatch_queue_t queue = dispatch_get_main_queue();
    
    dispatch_source_t write_source
        = dispatch_source_create(DISPATCH_SOURCE_TYPE_WRITE,
                                 context->fd,
                                 0,
                                 queue);
    context->write_source = write_source;
    dispatch_set_context(write_source, context);
    dispatch_source_set_event_handler_f
        (write_source, (void (*)(void *)) write_event);
    dispatch_source_set_timer(write_source,
                              context->request_time_initiated,
                              global_context->timeout,
                              0);
    dispatch_resume(write_source);
}


void terminate_connection(struct context *context) {
    if(context->fd) {
        close(context->fd);
        context->fd = -1;
    }
    
    if(context->initiate_source) {
        dispatch_release(context->initiate_source);
        context->initiate_source = NULL;
    }
    
    if(context->create_source) {
        dispatch_release(context->create_source);
        context->create_source = NULL;
    }
    
    if(context->write_source) {
        dispatch_release(context->write_source);
        context->write_source = NULL;
    }
    
    if(context->read_source) {
        dispatch_release(context->read_source);
        context->read_source = NULL;
    }

    struct global_context *global_context = context->global_context;

    global_context->n_remaining_connections--;
    
    if(global_context->n_remaining_connections == 0)
        finish_measuring(global_context);
}


int handle_timeout_and_errors(struct context *context) {
    dispatch_time_t now = dispatch_time(DISPATCH_TIME_NOW, 0);
    
    struct global_context *global_context = context->global_context;
    
    if(now - context->request_time_initiated > global_context->timeout) {
        context->response_timed_out = 1;
        context->response_time_completed = now;
        
        terminate_connection(context);
        
        return 1;
    }
    
    if(context->fd == -1)
        return 1;
    
    int fd_error = 0;
    socklen_t fd_error_size = sizeof(fd_error);
    getsockopt(context->fd,
               SOL_SOCKET,
               SO_ERROR,
               &fd_error,
               &fd_error_size);
    if(fd_error) {
        context->request_failed = 1;
        
        switch(fd_error) {
        case ECONNREFUSED:
            break;
        case ECONNRESET:
            context->response_disconnected = 1;
            break;
        default:
            printf("Unexpected network error: %i\n", fd_error);
            break;
        }
        
        terminate_connection(context);
        
        return 1;
    }
    
    return 0;
}


void write_event(struct context *context) {
    if(handle_timeout_and_errors(context))
        return;
    
    struct global_context *global_context = context->global_context;
    
    ssize_t send_result = send(context->fd,
                               context->request_text_unsent,
                               context->request_text_unsent_size,
                               0);
    if(send_result == -1) {
        return;
    }
    context->request_text_unsent += send_result;
    context->request_text_unsent_size -= send_result;
    if(context->request_text_unsent_size > 0) {
        return;
    }
    
    context->request_sent = 1;
    
    dispatch_release(context->write_source);
    context->write_source = NULL;
    
    dispatch_queue_t queue = dispatch_get_main_queue();
    dispatch_source_t read_source
        = dispatch_source_create(DISPATCH_SOURCE_TYPE_READ,
                                 context->fd,
                                 0,
                                 queue);
    context->read_source = read_source;
    dispatch_set_context(read_source, (void *) context);
    dispatch_source_set_event_handler_f
        (read_source, (void (*)(void *)) read_event);
    dispatch_source_set_timer(read_source,
                              context->request_time_initiated,
                              global_context->timeout,
                              0);
    dispatch_resume(read_source);
}


void read_event(struct context *context) {
    if(handle_timeout_and_errors(context))
        return;
    
    dispatch_source_t source = context->read_source;
    
    context->response_timed_out = 1;
    terminate_connection(context);
}
