/*
# Copyright 2025 University of Kentucky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
*/

/* 
Please specify the group members here

# Student #1: Sing Hieng (James) Wong
# Student #2: Ayah Abdeldayem
# Student #3: Hannah Hale

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>
#include <errno.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

/*
 * This structure is used to store per-thread data in the client
 */
typedef struct
{
    int epoll_fd;        /* File descriptor for the epoll instance, used for monitoring events on the socket. */
    int socket_fd;       /* File descriptor for the client socket connected to the server. */
    long long total_rtt; /* Accumulated Round-Trip Time (RTT) for all messages sent and received (in microseconds). */
    long total_messages; /* Total number of messages sent and received. */
    float request_rate;  /* Computed request rate (requests per second) based on RTT and total messages. */
    long tx_cnt;
    long rx_cnt;
    struct sockaddr_in server_addr;
} client_thread_data_t;

/*
 * This function runs in a separate client thread to handle communication with the server
 */
void *client_thread_func(void *arg)
{
    client_thread_data_t *data = (client_thread_data_t *)arg;
    struct epoll_event event, events[MAX_EVENTS];
    char write_buf[MESSAGE_SIZE] = "ABCDEFGHIJKMLNOP"; /* Send 16-Bytes message every time */
    char read_buf[MESSAGE_SIZE];
    struct timeval start, end; // Timestamps for RTT measurement
    socklen_t addr_len = sizeof(data->server_addr);

    /* TODO:
     * It sends messages to the server, waits for a response using epoll,
     * and measures the round-trip time (RTT) of this request-response.
     * The function exits after sending and receiving a predefined number of messages (num_requests). 
     * It calculates the request rate based on total messages and RTT
     */

    // Register socket with epoll for just read events
    event.events = EPOLLIN;
    event.data.fd = data->socket_fd;
    if (epoll_ctl(data->epoll_fd, EPOLL_CTL_ADD, data->socket_fd, &event) == -1)
    {
        perror("epoll_ctl: data->socket_fd");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < num_requests; i++)
    {
        gettimeofday(&start, NULL);
        sendto(data->socket_fd, write_buf, MESSAGE_SIZE, 0, (struct sockaddr *)&data->server_addr, addr_len);
        data->tx_cnt++;

        int nfds = epoll_wait(data->epoll_fd, events, MAX_EVENTS, 1000);
        if (nfds == -1)
        {
            perror("epoll_wait");
            exit(EXIT_FAILURE);
        }

        // if (nfds == 0)
        // {
        //     // timeout (packet loss)
        //     continue;
        // }

        ssize_t rcv = recvfrom(data->socket_fd, read_buf, MESSAGE_SIZE, 0, NULL, NULL);
        if (rcv == MESSAGE_SIZE) // only count complete messages
        {
            gettimeofday(&end, NULL);

            // record the total_rtt in microseconds
            data->total_rtt += (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);

            data->rx_cnt++;
            data->total_messages++;
        }
    }

    // request_rate in messages per second
    data->request_rate = (float)data->total_messages / ((float)data->total_rtt / 1000000);

    close(data->socket_fd);
    close(data->epoll_fd);

    return NULL;
}

/*
 * This function orchestrates multiple client threads to send requests to a server,
 * collect performance data of each threads, and compute aggregated metrics of all threads.
 */
void run_client()
{
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];

    // Create and initialize client threads
    for (int i = 0; i < num_client_threads; i++)
    {
        // Create epoll instance
        thread_data[i].epoll_fd = epoll_create1(0);
        if (thread_data[i].epoll_fd == -1)
        {
            perror("epoll_create1");
            exit(EXIT_FAILURE);
        }

        // Create UDP socket
        thread_data[i].socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (thread_data[i].socket_fd == -1)
        {
            perror("socket");
            exit(EXIT_FAILURE);
        }

        // Initialize server address structure for each thread for thread safety
        memset(&thread_data[i].server_addr, 0, sizeof(thread_data[i].server_addr));
        thread_data[i].server_addr.sin_family = AF_INET;
        thread_data[i].server_addr.sin_addr.s_addr = inet_addr(server_ip);
        thread_data[i].server_addr.sin_port = htons(server_port);

        thread_data[i].total_messages = 0;
        thread_data[i].total_rtt = 0;
        thread_data[i].tx_cnt = 0;
        thread_data[i].rx_cnt = 0;

        pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]);
    }

    /* TODO:
     * Wait for client threads to complete and aggregate metrics of all client threads
     */
    long long total_rtt = 0;
    long total_messages = 0;
    float total_request_rate = 0;
    long total_tx = 0, total_rx = 0;

    for (int i = 0; i < num_client_threads; i++)
    {
        pthread_join(threads[i], NULL);
        total_rtt += thread_data[i].total_rtt;
        total_messages += thread_data[i].total_messages;
        total_request_rate += thread_data[i].request_rate;
        total_tx += thread_data[i].tx_cnt;
        total_rx += thread_data[i].rx_cnt;
        printf("Thread %d complete, Sent: %ld, Received: %ld, Lost: %ld\n", i, thread_data[i].tx_cnt, thread_data[i].rx_cnt, thread_data[i].tx_cnt - thread_data[i].rx_cnt);
    }

    printf("Average RTT: %lld us\n", total_rtt / (total_messages > 0 ? total_messages : 1));
    printf("Total Request Rate: %f messages/s\n", total_request_rate);
    printf("Total Packets Sent: %ld, Received: %ld, Lost: %ld\n", total_tx, total_rx, total_tx - total_rx);
}

void run_server()
{
    printf("UDP Server started!\n");

    /* TODO:
     * Server creates listening socket and epoll instance.
     * Server registers the listening socket to epoll
     */
    int sockfd, nfds, epollfd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);
    struct epoll_event event, events[MAX_EVENTS];
    char buf[MESSAGE_SIZE];

    // Create socket
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd == -1)
    {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    // Initialize server address structure
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(server_port);

    // Bind listening socket to server address
    if (bind(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1)
    {
        perror("bind");
        exit(EXIT_FAILURE);
    }

    epollfd = epoll_create1(0);
    if (epollfd == -1)
    {
        perror("epoll_create1");
        exit(EXIT_FAILURE);
    }

    event.events = EPOLLIN;
    event.data.fd = sockfd;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sockfd, &event) == -1)
    {
        perror("epoll_ctl: sockfd");
        exit(EXIT_FAILURE);
    }

    /* Server's run-to-completion event loop */
    while (1)
    {
        nfds = epoll_wait(epollfd, events, MAX_EVENTS, -1);
        if (nfds == -1)
        {
            perror("epoll_wait");
            exit(EXIT_FAILURE);
        }

        if (nfds == 0)
        {
            // timeout = packet loss
            continue;
        }

        for (int n = 0; n < nfds; ++n)
        {
            if (events[n].data.fd == sockfd)
            {
                int len = recvfrom(sockfd, buf, MESSAGE_SIZE, 0,
                                   (struct sockaddr *)&client_addr, &client_len);

                if (len > 0) // only echo back non-empty messages
                {
                    sendto(sockfd, buf, MESSAGE_SIZE, 0, (struct sockaddr *)&client_addr, client_len);
                }
            }
        }
    }
}

int main(int argc, char *argv[])
{
    if (argc > 1 && strcmp(argv[1], "server") == 0)
    {
        if (argc > 2)
            server_ip = argv[2];
        if (argc > 3)
            server_port = atoi(argv[3]);

        run_server();
    }
    else if (argc > 1 && strcmp(argv[1], "client") == 0)
    {
        if (argc > 2)
            server_ip = argv[2];
        if (argc > 3)
            server_port = atoi(argv[3]);
        if (argc > 4)
            num_client_threads = atoi(argv[4]);
        if (argc > 5)
            num_requests = atoi(argv[5]);

        run_client();
    }
    else
    {
        printf("Usage: %s <server|client> [server_ip server_port num_client_threads num_requests]\n", argv[0]);
    }

    return 0;
}
