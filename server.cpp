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

    close(server_fd);

    return 0;
}

