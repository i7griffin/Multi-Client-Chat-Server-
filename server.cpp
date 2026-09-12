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
// to prevent race condition
mutex cout_mutex;
mutex clients_mutex;

vector<int> client_fds;
/*this global value is there to restrict the users in the server ,
to chat within the given message limit*/
const size_t MAX_MESSAGE_SIZE = 1024;

bool recv_exact(int fd, char *buffer, size_t num_bytes);
bool send_exact(int fd, const char *buffer, size_t num_bytes);
bool send_message(int fd, const string &message);
bool recv_message(int fd, string &out_message);

bool send_exact(int fd, const char *buffer, size_t num_bytes)
{
    size_t bytes_sent = 0;

    while (bytes_sent < num_bytes)
    {
        ssize_t result = send(
            fd,
            buffer + bytes_sent,
            num_bytes - bytes_sent,
            0);

        if (result == -1)
        {
            return false;
        }

        if (result == 0)
        {
            return false;
        }

        bytes_sent += result;
    }

    return true;
}

bool send_message(int fd, const string &message)
{
    uint32_t message_length = message.length();

    if (message_length > MAX_MESSAGE_SIZE)
    {
        return false;
    }

    // htonl means home to network long
    /*we need to convert it because irrespective of what order each computer use , the
    standard network byte order is big endian */

    /*But the number is represented in your computer's host byte order.

    htonl() converts the 32-bit value to network byte order.*/
    uint32_t network_length = htonl(message_length);

    bool success = send_exact(fd, reinterpret_cast<const char *>(&network_length), sizeof(network_length));

    if (!success)
    {
        return false;
    }

    if (message_length == 0)
    {
        return true;
    }

    success = send_exact(fd, message.data(), message_length);

    if (!success)
    {
        return false;
    }

    return true;
}

bool recv_message(int fd, string &out_message)
{
    uint32_t network_length;

    bool success = recv_exact(fd, reinterpret_cast<char *>(&network_length), sizeof(network_length));

    if (!success)
    {
        return false;
    }

    // ntohl means network to host length address
    uint32_t message_length = ntohl(network_length);

    if (message_length > MAX_MESSAGE_SIZE)
    {
        return false;
    }

    out_message.resize(message_length);

    if (message_length == 0)
    {
        return true;
    }

    success = recv_exact(fd, out_message.data(), message_length);

    if (!success)
    {
        out_message.clear();
        return false;
    }

    return true;
}

bool recv_exact(int fd, char *buffer, size_t num_bytes)
{
    size_t bytes_received = 0;

    while (bytes_received < num_bytes)
    {
        ssize_t result = recv(
            fd,
            buffer + bytes_received,
            num_bytes - bytes_received,
            0);

        if (result == 0)
        {
            return false;
        }

        if (result == -1)
        {
            return false;
        }

        bytes_received += result;
    }

    return true;
}

void handle_client(int client_fd)
{
    while (true)
    {
        // now using recv_message() instead of raw recv() so the length-prefixed
        // protocol is actually respected here, not just in send_message/recv_message themselves
        string message;

        bool ok = recv_message(client_fd, message);

        if (!ok)
        {
            lock_guard<mutex> lock(cout_mutex);
            /*RAII stands for Resource Acquisition Is Initialization
            lock gaurd is a RAII object which handles the resource lifetime over
            objects' lifetime destroys the resource after it goes out of scope
            no need to manually unlock after the critical part of the code*/
            cout << "Client disconnected or framing error." << endl;
            break;
        }

        {
            lock_guard<mutex> lock(cout_mutex);

            cout << "Received: " << message << endl;
        }

        bool send_failed = false;
        int failed_fd = -1;

        {
            lock_guard<mutex> lock(clients_mutex);

            for (int fd : client_fds)
            {
                if (fd == client_fd)
                {
                    continue;
                }

                // now using send_message() so the broadcast is also framed correctly
                bool sent_ok = send_message(fd, message);

                if (!sent_ok)
                {
                    send_failed = true;
                    failed_fd = fd;
                }
            }
        }

        if (send_failed)
        {
            lock_guard<mutex> lock(cout_mutex);

            cout << "send_message failed for client " << failed_fd << endl;
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