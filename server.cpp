#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
// header for socaddr_in and htons
#include <netinet/in.h>
#include <thread>
#include <mutex>
#include <vector>

using namespace std;

/*two different locks for the clients array and separate lock for cout .the locks for the clients array 
for the fd to removed or added only by one thread and prevents other thread trying to access it at the same time .*/
//to prevent race condition
mutex cout_mutex;
mutex clients_mutex;

vector<int> client_fds;

void handle_client(int client_fd)
{
    while (true)
    {
        char message[1024];

        memset(message, 0, sizeof(message));

        ssize_t bytes_received = recv(client_fd, message, sizeof(message), 0);

        if (bytes_received == -1)
        {
            lock_guard<mutex> lock(cout_mutex);
            /*RAII stands for Resource Acquisition Is Initialization
            lock gaurd is a RAII object which handles the resource lifetime over
            objects' lifetime destroys the resource after it goes out of scope
            no need to manually unlock after the critical part of the code*/
            cout << "Receive failed: " << strerror(errno) << endl;
            break;
        }

        if (bytes_received == 0)
        {
            lock_guard<mutex> lock(cout_mutex);
            cout << "Client disconnected." << endl;
            break;
        }

        {
            lock_guard<mutex> lock(cout_mutex);

            cout << "Received: ";
            cout.write(message, bytes_received);
            cout << endl;
        }

        bool send_failed = false;
        int failed_fd = -1;
        int send_error = 0;

        {
            lock_guard<mutex> lock(clients_mutex);

            for (int fd : client_fds)
            {
                if (fd == client_fd)
                {
                    continue;
                }

                ssize_t bytes_sent = send(fd, message, bytes_received, 0);

                if (bytes_sent == -1)
                {
                    send_failed = true;
                    failed_fd = fd;
                    send_error = errno;
                }
            }
        }

        if (send_failed)
        {
            lock_guard<mutex> lock(cout_mutex);

            cout << "Send failed for client "
                 << failed_fd << ": "
                 << strerror(send_error) << endl;
        }
    }

    {
        lock_guard<mutex> lock(clients_mutex);

        for (auto it = client_fds.begin(); it != client_fds.end(); ++it)
        {
            if (*it == client_fd)
            {
                client_fds.erase(it);
                break;
            }
        }
    }

    close(client_fd);
}

int main()
{
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd == -1)
    {
        lock_guard<mutex> lock(cout_mutex);
        cout << "Error: " << strerror(errno) << endl;
        return 1;
    }

    {
        lock_guard<mutex> lock(cout_mutex);

        cout << "Socket created successfully!" << endl;
        cout << "File descriptor: " << server_fd << endl;
    }

    // this is the structure for server address
    struct sockaddr_in server_addr;

    // Zeroing out the memory of the structure
    memset(&server_addr, 0, sizeof(server_addr));

    // to listen through all available local interfaces
    server_addr.sin_addr.s_addr = INADDR_ANY;

    // AF_INET is for ipv4 addresses
    server_addr.sin_family = AF_INET;
    // htons converts the port number to network byte order
    server_addr.sin_port = htons(8080);

    // socket function creates the socket and the bind funciton gives the socket a local address
    // the bind function expects a pointer struck to serveraddr because the bing=d function must eb able to work with all
    // kinds of address families ,
    // so the general pointer of sockaddr is expected by the function
    int result = bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));

    // each and every thread have unique errno
    // and the function running in that thread can read or set that threads content
    if (result == -1)
    {
        lock_guard<mutex> lock(cout_mutex);
        cout << "Bind failed: " << strerror(errno) << endl;
        close(server_fd);
        return 1;
    }

    /*listen function has two parameters
    1.is the the file descriptor
    2.is the backlog paramater , this tells or enacts like the size of the waiting room of
    incoming connecctions from which the accept function works on
    i.e it is a queue of connections that have completed the TCP handshake
    */
    result = listen(server_fd, 5);

    if (result == -1)
    {
        lock_guard<mutex> lock(cout_mutex);
        cout << "Listen failed: " << strerror(errno) << endl;
        close(server_fd);
        return 1;
    }

    {
        lock_guard<mutex> lock(cout_mutex);
        cout << "The server is listening" << endl;
    }

    while (true)
    {
        sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);

        /*we are assigning a file descriptor to a client conenction request
        By default now the accept function is of the blocking type
        i.e it blocks the calling thread till connection is found */
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);

        if (client_fd == -1)
        {
            lock_guard<mutex> lock(cout_mutex);
            cout << "Accept failed: " << strerror(errno) << endl;
            continue;
        }

        {
            lock_guard<mutex> lock(cout_mutex);

            cout << "Client connected!" << endl;
            cout << "Client file descriptor: " << client_fd << endl;
        }

        {
            lock_guard<mutex> lock(clients_mutex);

            client_fds.push_back(client_fd);
        }

        /*we are creating an another thread and calling the handle client function
        with client file descriptor as parameter on another thread .
        because i do not want the main function itself to enter the function and stall or something*/
        thread client_thread(handle_client, client_fd);

        /*using detach of the particualr thread helps because it enables it to run on its own. even if the mainthread
        goes on to end */
        client_thread.detach();
    }

    close(server_fd);

    return 0;
}