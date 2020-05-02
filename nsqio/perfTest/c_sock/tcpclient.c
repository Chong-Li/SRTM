/*
 * tcpclient.c - A simple TCP client
 * usage: tcpclient <host> <port>
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <linux/types.h>
//#include <linux/spinlock.h>
#include <time.h>
#include <pthread.h>
#include <stdbool.h>

#define BUFSIZE 1000
#define MILLION 1000000L
#define BILLION 1000000000L
#define THOUSAND 1000
#define RECORDSIZE 10000
/*
 * error - wrapper for perror
 */

void printArray(long int array[], char fielname[],  int num);
void timespec_diff(struct timespec *start, struct timespec *stop,
                   struct timespec *result);

void error(char *msg) {
    perror(msg);
    exit(0);
}

int sockfd;

int stopCount = 5000000;
int pktLen = 1000;

long int recordbuf[RECORDSIZE];


int main(int argc, char **argv) {
    int portno, n;
    struct sockaddr_in serveraddr;
    struct hostent *server;
    char *hostname;
    char buf[BUFSIZE];

    /* check command line arguments */
    if (argc < 3 || argc > 5) {
       fprintf(stderr,"usage: %s <hostname> <port> [stopnumber] [pktlen]\n", argv[0]);
       exit(0);
    }
    hostname = argv[1];
    portno = atoi(argv[2]);

    // stop after sending and receiving stopCount packets
    if (argc > 3) {
      stopCount = atoi(argv[3]);
    }

    if (argc >4){
      pktLen = atoi(argv[4]);
    }
    //pthread_mutex_init(&lock_x, NULL);

    /* socket: create the socket */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        error("ERROR opening socket");

    /* gethostbyname: get the server's DNS entry */
    server = gethostbyname(hostname);
    if (server == NULL) {
        fprintf(stderr,"ERROR, no such host as %s\n", hostname);
        exit(0);
    }

    /* build the server's Internet address */
    bzero((char *) &serveraddr, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    bcopy((char *)server->h_addr,
    (char *)&serveraddr.sin_addr.s_addr, server->h_length);
    serveraddr.sin_port = htons(portno);

    /* connect: create a connection with the server */
    if (connect(sockfd, &serveraddr, sizeof(serveraddr)) < 0)
      error("ERROR connecting");

    /* create thread for print out the measure information for each second */
    //pthread_create(&report_thread.thread_id, NULL, measureReport, NULL);
    //pthread_create(&receiving_thread.thread_id, NULL, measureDelay, NULL);

    /* get message line from the user */
    bzero(buf, BUFSIZE);
    //fgets(buf, BUFSIZE, stdin);

    /* send the message line to the server */
    struct timespec startTime, endTime;
    struct timespec sendTime, recvTime;
    struct timespec result;
    int k;
    int recordCount = 0;
    int gap = stopCount/2/RECORDSIZE;
    clock_gettime(CLOCK_MONOTONIC, &startTime);
    int m = 0;
    for (k=1; k <= stopCount; k++){
      m = 0; n = 0;
      while (n != pktLen){
        m = read(sockfd, buf+n, BUFSIZE-n);
        if (m < 0)
          error("ERROR reading from socket");
        n += m;
      }
      clock_gettime(CLOCK_REALTIME, &recvTime);

      memcpy((void*)&sendTime , buf, sizeof(struct timespec));
      timespec_diff(&sendTime, &recvTime, &result);
      if (k >= stopCount/4 && k < stopCount*3/4 && k%gap == 0 && recordCount < RECORDSIZE){
        recordbuf[recordCount++] = (long int)result.tv_sec*BILLION + result.tv_nsec;
      }
    }
    clock_gettime(CLOCK_MONOTONIC, &endTime);
    shutdown(sockfd, SHUT_RDWR);
    timespec_diff(&startTime, &endTime, &result);
    printf("Time for running is %lld.%.9ld\n",(long long)result.tv_sec, result.tv_nsec);
    printArray(recordbuf,"tcp_latency.log", RECORDSIZE);
    return 0;
}

void timespec_diff(struct timespec *start, struct timespec *stop,
                   struct timespec *result)
{
    if ((stop->tv_nsec - start->tv_nsec) < 0) {
        result->tv_sec = stop->tv_sec - start->tv_sec - 1;
        result->tv_nsec = stop->tv_nsec - start->tv_nsec + 1000000000;
    } else {
        result->tv_sec = stop->tv_sec - start->tv_sec;
        result->tv_nsec = stop->tv_nsec - start->tv_nsec;
    }
    return;
}

void printArray(long int array[], char filename[],  int num)
{
     int i;
     FILE * file = fopen(filename,"w");      /* open the file in append mode */
     for (i=0; i<num; i++)
          fprintf(file,"%ld\n",*(array+i)); /* write */
     fclose(file);                       /* close the file pointer */
     return ;
}
