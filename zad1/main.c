/* Systemy Operacyjne 2018 Patryk Wegrzyn */

#define _POSIX_C_SOURCE 200809L

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <ctype.h>
#include <time.h>
#include <stdarg.h>
#include <sys/time.h>
#include <math.h>
#include <yaml.h>
#include <signal.h>

// Search mode for the magic number
typedef enum search_tag {
    LESS_THAN,
    GREATER_THAN,
    EQUAL
} search_mode;

// Log mode
typedef enum log_tag {
    FULL,
    SIMPLIFIED
} log_mode;

// Represents the parsed config options
typedef struct config_tag{
    int no_producers;
    int no_consumers;
    int array_size;
    char *source_file;
    int magic_length;
    search_mode s_mode;
    log_mode l_mode;
    int nk;
} config_options;

// GLOBALS
char **main_buffer;
int mb_count;
int last_insert_index;
int last_delete_index;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t full_cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t empty_cond = PTHREAD_COND_INITIALIZER;
config_options *options;
int eof_reached = 0;
int sig_received = 0;
FILE *source_file_handle;

// Helper function used to signalize argument errors
void sig_arg_err()
{
    printf("Wrong argument format.\n"
           "Usage: zad1 <config file>\n");
    exit(EXIT_FAILURE);
}

// Reads config options from the config file
config_options* parse_config_file(const char *file_path)
{
    config_options *result;
    char *no_producers_np, *no_consumers_np, *array_size_np, *magic_length_np;
    char *search_mode_np, *log_mode_np, *nk_np;
    FILE *file;
    yaml_parser_t parser;
    yaml_token_t token;

    result = (config_options*)malloc(sizeof(config_options));
    if(result == NULL)
    {
        perror("Error while allocating memory");
        exit(EXIT_FAILURE);
    }

    file = fopen(file_path, "r");
    if(file == NULL)
    {
        perror("Error while opening config file");
        exit(EXIT_FAILURE);
    }

    if (!yaml_parser_initialize(&parser)) 
    {
        fprintf(stderr, "Error while initializing parser\n");
        return NULL;
    }

    yaml_parser_set_input_file(&parser, file);

    int expect_value = 0;
    char **string_data;
    char *token_key;

    do
    {
        yaml_parser_scan(&parser, &token);
        switch(token.type)
        {
            case YAML_KEY_TOKEN:
                expect_value = 0;
                break;
            case YAML_VALUE_TOKEN:
                expect_value = 1;
                break;
            case YAML_SCALAR_TOKEN:
                token_key = (char*)token.data.scalar.value;
                if(!expect_value)
                {
                    if(!strcmp(token_key, "source_file"))
                        string_data = &(result->source_file);
                    else if(!strcmp(token_key, "number_of_producers"))
                        string_data = &no_producers_np;
                    else if(!strcmp(token_key, "number_of_consumers"))
                        string_data = &no_consumers_np;
                    else if(!strcmp(token_key, "pointer_array_size"))
                        string_data = &array_size_np;
                    else if(!strcmp(token_key, "magic_number_length"))
                        string_data = &magic_length_np;
                    else if(!strcmp(token_key, "search_mode"))
                        string_data = &search_mode_np;
                    else if(!strcmp(token_key, "log_mode"))
                        string_data = &log_mode_np;
                    else if(!strcmp(token_key, "nk"))
                        string_data = &nk_np;
                    else
                        printf("Unknown key in config file: %s\n", token_key);
                }
                else 
                    *string_data = strdup(token_key);
                break;
            default: break;
        }
        if(token.type != YAML_STREAM_END_TOKEN)
            yaml_token_delete(&token);
    } while (token.type != YAML_STREAM_END_TOKEN);
    
    yaml_token_delete(&token);
    yaml_parser_delete(&parser);

    result->array_size = atoi(array_size_np);
    result->magic_length = atoi(magic_length_np);
    result->nk = atoi(nk_np);
    result->no_consumers = atoi(no_consumers_np);
    result->no_producers = atoi(no_producers_np);
    switch(atoi(search_mode_np))
    {
        case -1:
            result->s_mode = LESS_THAN;
            break;
        case 0:
            result->s_mode = EQUAL;
            break;
        case 1:
            result->s_mode = GREATER_THAN;
            break;
        default: break;
    }
    switch(atoi(log_mode_np))
    {
        case 0:
            result->l_mode = SIMPLIFIED;
            break;
        case 1:
            result->l_mode = FULL;
            break;
        default: break;
    }
    
    fclose(file);
    return result;
}

// SIGINT handler (only used if nk == 0)
void sig_handler(int signo)
{
    const char *sigint_recv = "\nThe main thread has received a SIGINT interrupt - "
        "cancelling all other threads...\n";

    if(signo == SIGINT)
    {
        write(1, sigint_recv, strlen(sigint_recv));
        _exit(EXIT_SUCCESS);
    }
    if(signo == SIGALRM)
    {
        _exit(EXIT_SUCCESS);
    }
}

// Produces a new value and adds it to the buffer
int produce(void)
{
    char *buffer;
    
    buffer = (char*)calloc(512, sizeof(char));
    if(buffer == NULL)
    {
        fprintf(stderr, "Error while allocating memory in source file read buffer\n");
        exit(EXIT_FAILURE);
    }

    if(fgets(buffer, 512, source_file_handle) == NULL)
    {
        free(buffer);
        return -1;
    }

    buffer[strcspn(buffer, "\n")] = 0;

    if(options->l_mode == FULL)
        printf("PRODUCER: producing at index %d\n", last_insert_index);
    
    main_buffer[last_insert_index] = buffer;
    last_insert_index = (last_insert_index + 1) % options->array_size;
    mb_count++;
    return 0;
}

// Consumes a value from the buffer
void consume(void)
{
    if(options->l_mode == FULL)
        printf("CONSUMER: consuming at index %d\n", last_delete_index);

    switch(options->s_mode)
    {
        case LESS_THAN:
            if(strlen(main_buffer[last_delete_index]) < options->magic_length)
                printf("CONSUMER: Found at index: %d\n%s\n", last_delete_index, main_buffer[last_delete_index]);
            break;
        case EQUAL:
            if(strlen(main_buffer[last_delete_index]) == options->magic_length)
                printf("CONSUMER: Found at index: %d\n%s\n", last_delete_index, main_buffer[last_delete_index]);
            break;
        case GREATER_THAN:
            if(strlen(main_buffer[last_delete_index]) > options->magic_length)
                printf("CONSUMER: Found at index: %d\n%s\n", last_delete_index, main_buffer[last_delete_index]);
            break;
        default: break;
    }

    free(main_buffer[last_delete_index]);
    last_delete_index = (last_delete_index + 1) % options->array_size;
    mb_count--;
}

// Producer function
void* producer_init(void* args)
{
    if(options->l_mode == FULL)
        printf("PRODUCER: I have been created\n");
    
    sigset_t set;
    sigemptyset(&set);
    int err_code;

    if(options->nk == 0) 
        sigaddset(&set, SIGINT);
    else
        sigaddset(&set, SIGALRM);

    if((err_code=pthread_sigmask(SIG_BLOCK, &set, 0)) != 0)
    {
        fprintf(stderr, "Error while setting sigmask in producer\n");
        exit(EXIT_FAILURE);
    }

    while(!sig_received && !eof_reached)
    {
        pthread_mutex_lock(&mutex);

        while(mb_count >= options->array_size - 1)
            pthread_cond_wait(&full_cond, &mutex);

        if(produce() == -1)
        {
            eof_reached = 1;
        }

        pthread_cond_broadcast(&empty_cond);

        pthread_mutex_unlock(&mutex);
    }

    if(options->l_mode == FULL)
        printf("PRODUCER: I am done\n");

    return NULL;
}

// Consumer function
void* consumer_init(void* args)
{
    if(options->l_mode == FULL)
        printf("CONSUMER: I have been created\n");
    
    sigset_t set;
    sigemptyset(&set);
    int err_code;

    if(options->nk == 0) 
        sigaddset(&set, SIGINT);
    else
        sigaddset(&set, SIGALRM);

    if((err_code=pthread_sigmask(SIG_BLOCK, &set, 0)) != 0)
    {
        fprintf(stderr, "Error while setting sigmask in producer\n");
        exit(EXIT_FAILURE);
    }

    while(!sig_received)
    {
        pthread_mutex_lock(&mutex);
        
        while(mb_count == 0 && !eof_reached)
            pthread_cond_wait(&empty_cond, &mutex);

        if(eof_reached && !mb_count)
        {
            if(options->l_mode == FULL)
                printf("CONSUMER: I am done\n");
            
            pthread_mutex_unlock(&mutex);
            return NULL;
        }
        
        consume();

        pthread_cond_broadcast(&full_cond);

        pthread_mutex_unlock(&mutex);
    }

    if(options->l_mode == FULL)
        printf("CONSUMER: I am done\n");

    return NULL;
}

// Spawns the producer and consumer threads
void spawn_threads(pthread_t *producers, pthread_t *consumers)
{
    for(int i = 0; i < options->no_producers; i++)
    {
        if(pthread_create(&producers[i], NULL, (void *)producer_init, (void*)NULL) != 0)
        {
            fprintf(stderr, "Error while creating a new producer thread\n");
            exit(EXIT_FAILURE);
        }
    }
    for(int i = 0; i < options->no_consumers; i++)
    {
        if(pthread_create(&consumers[i], NULL, (void *)consumer_init, (void*)NULL) != 0)
        {
            fprintf(stderr, "Error while creating a new consumer thread\n");
            exit(EXIT_FAILURE);
        }
    }
}

// Waits on the producers and consumer threads
void join_threads(pthread_t *producers, pthread_t *consumers)
{
    int err_code;   
    for(int i = 0; i < options->no_producers; i++)
    {
        if((err_code=pthread_join(producers[i], NULL)) != 0)
        {
            fprintf(stderr, "Error while joining with a producer thread: %s\n",
                strerror(err_code));
            exit(EXIT_FAILURE);
        }
    }
    for(int i = 0; i < options->no_consumers; i++)
    {
        if((err_code=pthread_join(consumers[i], NULL)) != 0)
        {
            fprintf(stderr, "Error while joining with a consumer thread: %s\n", 
                strerror(err_code));
            exit(EXIT_FAILURE);
        }
    }
}

// MAIN Function
int main(int argc, char **argv)
{   
    if(argc < 2)
        sig_arg_err();
    else
        options = parse_config_file(argv[1]);

    if(options == NULL)
    {
        fprintf(stderr, "Error while parsing the config file\n");
        exit(EXIT_FAILURE);
    }

    main_buffer = (char**)malloc(sizeof(char*) * options->array_size);
    if(main_buffer == NULL)
    {
        fprintf(stderr, "Error while allocating memory for main buffer\n");
        exit(EXIT_FAILURE);
    }

    pthread_t *producers, *consumers;
    producers = (pthread_t*)malloc(sizeof(pthread_t) * options->no_producers);
    consumers = (pthread_t*)malloc(sizeof(pthread_t) * options->no_consumers);
    if(producers == NULL || consumers == NULL)
    {
        fprintf(stderr, "Error while allocating memory");
        exit(EXIT_FAILURE);
    }

    source_file_handle = fopen(options->source_file, "r");
    if(source_file_handle == NULL)
    {
        perror("Error while opening producers source file");
        exit(EXIT_FAILURE);
    }

    if(options->nk == 0)
    {
        if(signal(SIGINT, sig_handler) == SIG_ERR)
        {
            perror("Error while setting SIGINT handler");
            exit(EXIT_FAILURE);
        }
    }
    else
    {
        if(signal(SIGALRM, sig_handler) == SIG_ERR)
        {
            perror("Error while setting SIGALRM handler");
            exit(EXIT_FAILURE);
        }
        alarm(options->nk);
    }

    spawn_threads(producers, consumers);
    join_threads(producers, consumers);

    pthread_cond_destroy(&full_cond);
    pthread_cond_destroy(&empty_cond);
    pthread_mutex_destroy(&mutex);
    fclose(source_file_handle);
    free(producers);
    free(consumers);
    free(main_buffer);
    exit(EXIT_SUCCESS);
}