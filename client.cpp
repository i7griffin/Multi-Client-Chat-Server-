#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <arpa/inet.h>

using namespace std;

int main()
{
    // creating client side TCP socket
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (client_fd == -1)
    {
        cout << "Socket creation failed: "
             << strerror(errno) << endl;
        return 1;
    }

    cout << "Socket created successfully!" << endl;
    cout << "File descriptor: " << client_fd << endl;

    // Structure for having server_addr
    struct sockaddr_in server_addr;

    memset(&server_addr, 0, sizeof(server_addr));

    // AF_INET means that the ip address is of family IPv4
    server_addr.sin_family = AF_INET;

    // storing th evalue of the port for thw server
    server_addr.sin_port = htons(8080);

    // Converting the string ip address to actual binary or network representation
    int result = inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (result == 0)
    {
        cout << "Invalid IP address" << endl;
        close(client_fd);
        return 1;
    }

    if (result == -1)
    {
        cout << "inet_pton failed: " << strerror(errno) << endl;
        close(client_fd);
        return 1;
    }

    // function to connect to the server
    result = connect(client_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));

    if (result == -1)
    {
        cout << "Connection failed: "
             << strerror(errno) << endl;
        close(client_fd);
        return 1;
    }

    cout << "Connected to server!" << endl;

    // Creating the message to send to the server
    const char *message = "Hello from client!";

    // signed representation of the amount of the bytes to send to the server
    ssize_t bytes_sent = send(client_fd, message, strlen(message), 0);

    if (bytes_sent == -1)
    {
        cout << "Send failed: " << strerror(errno) << endl;
        close(client_fd);
        return 1;
    }

    cout << "Sent " << bytes_sent << " bytes" << endl;

    // creating the buffer to rexeive the server's echo back as a reply to the message sent
    char buffer[1024];

    // emptying or zeroing out the buffer to store the echo that is sent from the server
    memset(buffer, 0, sizeof(buffer));

    ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);

    if (bytes_received == -1)
    {
        cout << "Receive failed: " << strerror(errno) << endl;
        close(client_fd);
        return 1;
    }

    if (bytes_received == 0)
    {
        cout << "Server disconnected." << endl;
    }
    else
    {
        cout << "Server replied: ";
        cout.write(buffer, bytes_received);
        cout << endl;
    }

    close(client_fd);

    return 0;
}