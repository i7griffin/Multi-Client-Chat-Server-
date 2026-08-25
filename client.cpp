#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thread>
#include <mutex>

using namespace std;

mutex cout_mutex;

void receive_loop(int client_fd)
{
    while (true)
    {
        char buffer[1024];

        memset(buffer, 0, sizeof(buffer));

        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);

        if (bytes_received == -1)
        {
            {
                //this mutex loxk is for cout things 
                lock_guard<mutex> lock(cout_mutex);
                cout << "Receive failed: " << strerror(errno) << endl;
            }

            exit(0);
        }

        if (bytes_received == 0)
        {
            {
                lock_guard<mutex> lock(cout_mutex);
                cout << "Server disconnected." << endl;
            }

            exit(0);
        }

        {
            lock_guard<mutex> lock(cout_mutex);

            cout << "Server replied: ";
            cout.write(buffer, bytes_received);
            cout << endl;
        }
    }
}

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

    thread recv_thread(receive_loop, client_fd);
    recv_thread.detach();

    // this loop is to send message till the client doesnt want to send anymore
    while (true)
    {
        string line;

        getline(cin, line);

        if (line == "quit")
        {
            break;
        }

        ssize_t bytes_sent = send(client_fd, line.c_str(), line.length(), 0);

        if (bytes_sent == -1)
        {
            lock_guard<mutex> lock(cout_mutex);
            cout << "Send failed: " << strerror(errno) << endl;
            close(client_fd);
            return 1;
        }

        {
            lock_guard<mutex> lock(cout_mutex);
            cout << "Sent " << bytes_sent << " bytes" << endl;
        }
    }

    close(client_fd);

    return 0;
}