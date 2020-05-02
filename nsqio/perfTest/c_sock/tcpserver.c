/*
 * tcpserver.c - A simple TCP echo server
 * usage: tcpserver <port>
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <netinet/tcp.h>

#define BUFSIZE 1000
#define MILLION 1000000L
#define THOUSAND 1000

int stopCount = 5000000;
int pktLen = 1000;
int sendInterval = 0;

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

void error(char *msg) {
  perror(msg);
  exit(1);
}

int main(int argc, char **argv) {
  int parentfd; /* parent socket */
  int childfd; /* child socket */
  int portno; /* port to listen on */
  int clientlen; /* byte size of client's address */
  struct sockaddr_in serveraddr; /* server's addr */
  struct sockaddr_in clientaddr; /* client addr */
  char buf[BUFSIZE]; /* message buffer */
  char *hostaddrp; /* dotted decimal host addr string */
  int optval; /* flag value for setsockopt */
  int n; /* message byte size */

  /*
   * check command line arguments
   */
  if (argc < 2 || argc > 5) {
    fprintf(stderr, "usage: %s <port> [stopCount] [pktLen] [sendInterval]\n", argv[0]);
    exit(1);
  }
  portno = atoi(argv[1]);

  // stop after sending and receiving stopCount packets
  if (argc > 2) {
    stopCount = atoi(argv[2]);
  }

  if (argc >3){
    pktLen = atoi(argv[3]);
  }

  if (argc > 4){
    sendInterval = atoi(argv[4]);
  }

  /*
   * socket: create the parent socket
   */
  parentfd = socket(AF_INET, SOCK_STREAM, 0);
  if (parentfd < 0)
    error("ERROR opening socket");

  /* setsockopt: Handy debugging trick that lets
   * us rerun the server immediately after we kill it;
   * otherwise we have to wait about 20 secs.
   * Eliminates "ERROR on binding: Address already in use" error.
   */
  optval = 1;
  setsockopt(parentfd, SOL_SOCKET, SO_REUSEADDR,
       (const void *)&optval , sizeof(int));

  /*
   * build the server's Internet address
   */
  bzero((char *) &serveraddr, sizeof(serveraddr));

  /* this is an Internet address */
  serveraddr.sin_family = AF_INET;

  /* let the system figure out our IP address */
  serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);

  /* this is the port we will listen on */
  serveraddr.sin_port = htons((unsigned short)portno);

  /*
   * bind: associate the parent socket with a port
   */
  if (bind(parentfd, (struct sockaddr *) &serveraddr,
     sizeof(serveraddr)) < 0)
    error("ERROR on binding");

  /*
   * listen: make this socket ready to accept connection requests
   */
  if (listen(parentfd, 5) < 0) /* allow 5 requests to queue up */
    error("ERROR on listen");

  /*
   * main loop: wait for a connection request, echo input line,
   * then close connection.
   */

   double sendSpeed = sendInterval==0? 0 : 1.0*MILLION/sendInterval * pktLen * 8/1024/1024;

   printf("port number: %d\n", portno);
   printf("Stop Count: %d\t packet length: %d\n", stopCount, pktLen);
   printf("Sending Interval is %d us\n", sendInterval);
   printf("Sending Speed set to %.2f Mb/s\n\n", sendSpeed);
  clientlen = sizeof(clientaddr);
  while (1) {

    /*
     * accept: wait for a connection request
     */
    childfd = accept(parentfd, (struct sockaddr *) &clientaddr, (socklen_t *)&clientlen);
    if (childfd < 0)
      error("ERROR on accept");
    hostaddrp = inet_ntoa(clientaddr.sin_addr);
    if (hostaddrp == NULL)
      error("ERROR on inet_ntoa\n");
    printf("server established connection with %s\n",
      hostaddrp);
    bzero(buf, BUFSIZE);
    int one = 1;
    setsockopt(childfd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));

    int i = 0;
    struct timespec sendTime;
    struct timespec startTime, endTime;
    clock_gettime(CLOCK_MONOTONIC, &startTime);
    for ( i=0; i < stopCount; i++){
      clock_gettime(CLOCK_REALTIME, &sendTime);
      memcpy(buf, (const void*)&sendTime, sizeof(struct timespec));
      n = write(childfd, buf, pktLen);
      if (n < 0) {
        error("ERROR writing to socket");
        break;
      }
      if (sendInterval != 0)
        usleep(sendInterval);
    }
    clock_gettime(CLOCK_MONOTONIC, &endTime);
    printf("server connection disconnected.\n");
    struct timespec result;
    timespec_diff(&startTime, &endTime, &result);
    printf("Time for running is %lld.%.9ld\n",(long long)result.tv_sec, result.tv_nsec);
    close(childfd);
  }
}
