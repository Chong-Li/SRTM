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

#define BUFSIZE 1000
#define MILLION 1000000L
#define THOUSAND 1000

int stopCount = 5000000;
int pktLen = 1000;
int sendInterval = 0;

void timespec_diff(struct timespec *start, struct timespec *stop,
                    struct timespec *result);

/*
 * error - wrapper for perror
 */
void error(char *msg) {
  perror(msg);
  exit(1);
}

int main(int argc, char **argv) {
  int sockfd; /*  socket */
  int portno; /* port to listen on */
  struct sockaddr_in serveraddr; /* server's addr */
  struct sockaddr_in clientaddr; /* client addr */
  socklen_t addrlen = sizeof(clientaddr);

  char buf[BUFSIZE]; /* message buffer */
  int optval; /* flag value for setsockopt */
  int recvn, sendn; /* message byte size */

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
  sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0)
    error("ERROR opening socket");

  /* setsockopt: Handy debugging trick that lets
   * us rerun the server immediately after we kill it;
   * otherwise we have to wait about 20 secs.
   * Eliminates "ERROR on binding: Address already in use" error.
   */
  optval = 1;
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR,
       (const void *)&optval , sizeof(int));

  /*
   * build the server's Internet address
   */
  bzero((char *) &serveraddr, sizeof(serveraddr));

  /* this is an Internet address */
  serveraddr.sin_family = AF_INET;
  serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
  serveraddr.sin_port = htons((unsigned short)portno);

  /*
   * bind: associate the parent socket with a port
   */

   int sendbuff = 4*1024*1024; //4M
   setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sendbuff, sizeof(sendbuff));
   setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &sendbuff, sizeof(sendbuff));

  if (bind(sockfd, (struct sockaddr *) &serveraddr,
     sizeof(serveraddr)) < 0)
    error("ERROR on binding");

  /*
   * main loop: wait for a connection request, echo input line,
   * then close connection.
   */
  recvn = recvfrom(sockfd, buf, BUFSIZE, 0, (struct sockaddr *)&clientaddr, &addrlen);
  if (recvn < 0) {
    error("ERROR reading from socket");
  }

  struct timespec sendTime;
  struct timespec startTime, endTime;
  clock_gettime(CLOCK_MONOTONIC, &startTime);
  int i = 0;
  for (i=0; i < stopCount; i++) {
    clock_gettime(CLOCK_REALTIME, &sendTime);
    memcpy(buf, (const void*)&sendTime, sizeof(struct timespec));
    memcpy(buf+sizeof(struct timespec), (const void*)&i, sizeof(int));
    sendn = sendto(sockfd, buf, pktLen, 0, (struct sockaddr *)&clientaddr, addrlen);
    if (sendn < 0) {
      error("ERROR writing to socket");
    }
    if (sendInterval != 0)
      usleep(sendInterval);
  }
  clock_gettime(CLOCK_MONOTONIC, &endTime);
  printf("server connection disconnected.\n");
  struct timespec result;
  timespec_diff(&startTime, &endTime, &result);
  printf("Time for running is %lld.%.9ld\n",(long long)result.tv_sec, result.tv_nsec);
  usleep(1);
  int errcode = -1;
  memcpy(buf+sizeof(struct timespec), (const void*)&errcode, sizeof(int));
  sendn = sendto(sockfd, buf, pktLen, 0, (struct sockaddr *)&clientaddr, addrlen);
  close(sockfd);
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
