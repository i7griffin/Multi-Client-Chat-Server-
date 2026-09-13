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

const size_t MAX_MESSAGE_SIZE = 1024;

// Must match the server's enum exactly - same values, same meaning - since
// this byte is what tells the other side "this is the one-time username
// handshake" vs "this is a regular chat message".
enum class MessageType : uint8_t
{
    USERNAME = 0,
    CHAT = 1
};

bool recv_exact(int fd, char *buffer, size_t num_bytes)
{
    size_t bytes_received = 0;

    while (bytes_received < num_bytes)
    {
        ssize_t result = recv(fd,buffer + bytes_received,num_bytes - bytes_received,0);

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

bool send_message(int fd, MessageType type, const string &message)
{
    // Send the type byte first. One byte, so no htonl needed - endianness
    // only matters for multi-byte values like the length header below.
    uint8_t type_byte = static_cast<uint8_t>(type);

    bool type_sent = send_exact(fd, reinterpret_cast<const char *>(&type_byte), sizeof(type_byte));

    if (!type_sent)
    {
        return false;
    }

    uint32_t message_length = message.length();

    if (message_length > MAX_MESSAGE_SIZE)
    {
        return false;
    }

    // htonl means home to network long
    /*we need to convert it because irrespective of what order each computer use , the
    standard network byte order is big endian */
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

bool recv_message(int fd, MessageType &out_type, string &out_message)
{
    // Read the type byte first - same story as send_message(), no
    // endianness conversion needed for a single byte.
    uint8_t type_byte;

    bool type_ok = recv_exact(fd, reinterpret_cast<char *>(&type_byte), sizeof(type_byte));

    if (!type_ok)
    {
        return false;
    }

    out_type = static_cast<MessageType>(type_byte);

    uint32_t network_length;

    bool success = recv_exact(
        fd,
        reinterpret_cast<char *>(&network_length),
        sizeof(network_length));

    if (!success)
    {
        return false;
    }

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

    success = recv_exact(
        fd,
        out_message.data(),
        message_length);

    if (!success)
    {
        out_message.clear();
        return false;
    }

    return true;
}

void receive_loop(int client_fd)
{
    while (true)
    {
        // This client only ever expects CHAT messages back from the server
        // (the server only broadcasts chat, never sends USERNAME messages),
        // so the type isn't used for anything here - it still has to be
        // passed, since recv_message() needs somewhere to put it.
        MessageType type;
        string message;

        bool success = recv_message(client_fd, type, message);

        if (!success)
        {
            {
                // this mutex loxk is for cout things
                lock_guard<mutex> lock(cout_mutex);

                cout << "Receive failed or server disconnected." << endl;
            }

            exit(0);
        }

        {
            lock_guard<mutex> lock(cout_mutex);

            cout << "Server replied: ";
            cout << message;
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

    // One-time handshake: register a username with the server before doing
    // anything else. This has to happen before the receive thread starts
    // and before any chat messages go out, since the server expects exactly
    // one USERNAME message first, on this same connection.
    string username;

    cout << "Enter username: ";

    // If stdin hits EOF right here (e.g. piped input with nothing left),
    // getline() fails and leaves username untouched/empty - checking the
    // return value lets us bail out cleanly instead of trying to proceed
    // with a connection that never got a real handshake.
    if (!getline(cin, username))
    {
        cout << "No username entered; exiting." << endl;
        close(client_fd);
        return 1;
    }

    {
        bool username_sent = send_message(client_fd, MessageType::USERNAME, username);

        if (!username_sent)
        {
            cout << "Failed to send username." << endl;
            close(client_fd);
            return 1;
        }
    }

    thread recv_thread(receive_loop, client_fd);
    recv_thread.detach();

    // this loop is to send message till the client doesnt want to send anymore
    while (true)
    {
        string line;

        // Same fix as above: if getline() fails (stdin closed/EOF) without
        // the user ever typing "quit", break out instead of spinning forever
        // sending empty messages - getline() converts to false on failure,
        // which is exactly the case a bare "getline(cin, line);" call with
        // no check was silently ignoring before.
        if (!getline(cin, line))
        {
            break;
        }

        if (line == "quit")
        {
            break;
        }

        bool success = send_message(client_fd, MessageType::CHAT, line);

        if (!success)
        {
            lock_guard<mutex> lock(cout_mutex);

            cout << "Send failed." << endl;

            close(client_fd);
            return 1;
        }

        {
            lock_guard<mutex> lock(cout_mutex);

            cout << "Message sent successfully." << endl;
        }
    }

    close(client_fd);

    return 0;
}