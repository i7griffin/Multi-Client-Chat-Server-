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

    close(server_fd);

    return 0;
}
