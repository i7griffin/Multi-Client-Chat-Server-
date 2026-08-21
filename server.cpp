#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

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


    //this is the structure for server address
    struct sockaddr_in server_addr;

    // Zeroing out the memory of the structure
    memset(&server_addr, 0, sizeof(server_addr));

    // AF_INET is for ipv4 addresses
    server_addr.sin_family = AF_INET;
    //htons converts the port number to network byte order 
    server_addr.sin_port = htons(8080); 

    close(server_fd);

    return 0;
}

