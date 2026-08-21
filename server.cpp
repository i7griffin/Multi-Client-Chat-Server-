#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
// header for socaddr_in and htons
#include <netinet/in.h>

using namespace std;

int main()
{
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd == -1)
    {
        cout << "Error: " << strerror(errno) << endl;
        return 1;
    }

    cout << "Socket created successfully!" << endl;
    cout << "File descriptor: " << server_fd << endl;

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
        cout << "Listen failed: " << strerror(errno) << endl;
        close(server_fd);
        return 1;
    }

    cout << "The server is listening" << endl;

    sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    /*we are assigning a file descriptor to a client conenction request
    By default now the accept function is of the blocking type
    i.e it blocks the calling thread till connection is found */
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);

    if (client_fd == -1)
    {
        cout << "Accept failed: " << strerror(errno) << endl;
        close(server_fd);
        return 1;
    }

    cout << "Client connected!" << endl;
    cout << "Client file descriptor: " << client_fd << endl;

    char message[1024];

    // emptying the message block before writing new data
    memset(message, 0, sizeof(message));

    ssize_t bytes_received = recv(client_fd, message, sizeof(message), 0);

    if (bytes_received == -1)
    {
        cout << "Receive failed: " << strerror(errno) << endl;
        close(client_fd);
        close(server_fd);
        return 1;
    }

    if (bytes_received == 0)
    {
        cout << "Client disconnected." << endl;
    }
    else
    {
        cout << "Received: ";
        cout.write(message, bytes_received);
        cout << endl;

        ssize_t bytes_sent = send(client_fd, message, bytes_received, 0);

        if (bytes_sent == -1)
        {
            cout << "Send failed: " << strerror(errno) << endl;
        }
        else
        {
            cout << "Sent " << bytes_sent << " bytes" << endl;
        }
    }

    close(client_fd);
    close(server_fd);

    return 0;
}
