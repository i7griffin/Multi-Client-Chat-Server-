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
#include <csignal>
#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <map>

using namespace std;

//atomic bool is used for shutdown is to ask the compile to check for other wayt o check the modified cvalue of he flag and not to check only in the flow of program 
atomic<bool> shutdown_flag(false);

//signal handler is what we use to change the flag value outside of the program execution flow 
void signal_handler(int signum)
{
    shutdown_flag = true;
}

/*two different locks for the clients array and separate lock for cout .the locks for the clients array
for the fd to removed or added only by one thread and prevents other thread trying to access it at the same time .*/
// to prevent race condition
mutex cout_mutex;
mutex clients_mutex;

vector<int> client_fds;

// Maps a connected client's fd to the username they registered at connect
// time. This is the same conceptual "who's connected" state as client_fds,
// just richer, so it's protected by the same clients_mutex rather than a
// separate lock - that avoids any risk of the two getting out of sync under
// concurrent access.
map<int, string> usernames;
/*this global value is there to restrict the users in the server ,
to chat within the given message limit*/
const size_t MAX_MESSAGE_SIZE = 1024;

// Log levels, ordered roughly by how much attention they deserve.
// INFO      - normal, expected lifecycle events (connect, disconnect, startup)
// WARN      - something failed but the server keeps running fine (a broadcast send failed)
// ERROR     - a syscall failed in a way that affects server operation (bind/listen/accept failed)
// SECURITY  - a client did something that looks like probing/attacking the protocol
//             (claiming an oversized message length), as opposed to just disconnecting)
enum class LogLevel
{
    INFO,
    WARN,
    ERROR,
    SECURITY
};

string level_to_string(LogLevel level)
{
    switch (level)
    {
        case LogLevel::INFO:
            return "INFO";
        case LogLevel::WARN:
            return "WARN";
        case LogLevel::ERROR:
            return "ERROR";
        case LogLevel::SECURITY:
            return "SECURITY";
    }

    return "UNKNOWN";
}

// Builds a "[YYYY-MM-DD HH:MM:SS] [LEVEL] message" line and prints it under
void log(LogLevel level, const string &message)
{
    auto now = chrono::system_clock::now();
    time_t now_time_t = chrono::system_clock::to_time_t(now);

    // localtime() is not thread-safe on its own (it can return a pointer to
    // shared static storage), so we use localtime_r, which fills a
    // caller-provided struct instead.


    /*struct tm
{
    int tm_sec;    // seconds
    int tm_min;    // minutes
    int tm_hour;   // hours
    int tm_mday;   // day of month
    int tm_mon;    // month
    int tm_year;   // year
};
*/
    tm local_tm{};
    localtime_r(&now_time_t, &local_tm);

    //ostringstream is a tool for constructing strings using << instead of repeatedly using string concatenation.
    ostringstream timestamp_stream;
    timestamp_stream << put_time(&local_tm, "%Y-%m-%d %H:%M:%S");

    lock_guard<mutex> lock(cout_mutex);
    // cout_mutex, so every log line (from any thread) is atomic and consistent 
    cout << "[" << timestamp_stream.str() << "] "
         << "[" << level_to_string(level) << "] "
         << message << endl;
}

// The type byte lets receivers tell "this is the one-time username handshake"
// apart from "this is a regular chat message" without needing a second,
// differently-shaped protocol. It's a single byte, so it has no endianness
// to worry about (endianness only matters for multi-byte values like our
// 4-byte length header).
enum class MessageType : uint8_t
{
    USERNAME = 0,
    CHAT = 1
};

bool recv_exact(int fd, char *buffer, size_t num_bytes);
bool send_exact(int fd, const char *buffer, size_t num_bytes);
bool send_message(int fd, MessageType type, const string &message);
bool recv_message(int fd, MessageType &out_type, string &out_message, bool *was_oversized = nullptr);

bool send_exact(int fd, const char *buffer, size_t num_bytes)
{
    size_t bytes_sent = 0;

    while (bytes_sent < num_bytes)
    {
        ssize_t result = send(fd, buffer + bytes_sent, num_bytes - bytes_sent, 0);

        if (result == -1)
        {
            if (errno == EINTR) continue;   
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
    // Send the type byte first. It's a single uint8_t, so it goes over the
    // wire as-is - no htonl needed, since byte order only matters for values
    // wider than one byte.
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

// was_oversized lets the caller tell "client claimed a length bigger than
// MAX_MESSAGE_SIZE" apart from "ordinary disconnect or read error". If the
// caller doesn't care (passes nullptr), behavior is unchanged from before.
bool recv_message(int fd, MessageType &out_type, string &out_message, bool *was_oversized)
{
    if (was_oversized != nullptr)
    {
        *was_oversized = false;
    }

    // Read the type byte first - same story as send_message(), no endianness
    // conversion needed for a single byte.
    uint8_t type_byte;

    bool type_ok = recv_exact(fd, reinterpret_cast<char *>(&type_byte), sizeof(type_byte));

    if (!type_ok)
    {
        return false;
    }

    out_type = static_cast<MessageType>(type_byte);

    //netwrok length is of standard length 4 bytes we are fixing a standard message fixed length for sending message across the server 

    //for recv exact function we will pass network_length as parameter
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
        if (was_oversized != nullptr)
        {
            *was_oversized = true;
        }

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
        /*buffer + bytes_received === will set offset from were the result should be read and stores*/
        ssize_t result = recv(fd,buffer + bytes_received,num_bytes - bytes_received,0);

        if (result == 0)
        {
            return false;
        }

        if (result == -1)
        {
            if (errno == EINTR) continue;   
            return false;
        }

        bytes_received += result;
    }

    return true;
}

void handle_client(int client_fd)
{
    // Before entering the normal chat loop, this client gets exactly one
    // chance to register a username. This runs once, outside the loop,
    // because it's a connection-setup step, not a recurring chat message.
    {
        MessageType handshake_type;
        string username;
        bool was_oversized = false;

        bool handshake_ok = recv_message(client_fd, handshake_type, username, &was_oversized);

        // Anything going wrong here - disconnect, oversized claim, or the
        // client sending CHAT instead of USERNAME first - means this
        // connection never got set up correctly, so we treat it the same
        // way as any other setup failure: log it and close, without ever
        // reaching the main loop or client_fds.
        if (!handshake_ok || handshake_type != MessageType::USERNAME)
        {
            ostringstream oss;
            oss << "Client fd " << client_fd << " failed the username handshake"
                << (was_oversized ? " (oversized message length)" : (!handshake_ok ? " (disconnected)" : " (wrong message type)"))
                << ". Closing connection.";
            log(LogLevel::INFO, oss.str());
            close(client_fd);
            return;
        }

        {
            lock_guard<mutex> lock(clients_mutex);
            usernames[client_fd] = username;
        }

        ostringstream oss;
        oss << "Client fd " << client_fd << " registered as \"" << username << "\".";
        log(LogLevel::INFO, oss.str());
    }

    while (true)
    {
        // now using recv_message() instead of raw recv() so the length-prefixed
        // protocol is actually respected here, not just in send_message/recv_message themselves
        MessageType type;
        string message;
        bool was_oversized = false;

        bool ok = recv_message(client_fd, type, message, &was_oversized);

        if (!ok)
        {
            // Distinguish an oversized-length claim (a protocol violation that
            // looks like probing/attacking the server) from an ordinary
            // disconnect or read error, which is routine and expected .


            /*the disconnection of the client from the server is handles properly*/
            if (was_oversized)
            {
                ostringstream oss;
                oss << "Client fd " << client_fd
                    << " claimed an oversized message length (> "
                    << MAX_MESSAGE_SIZE << " bytes). Dropping connection.";
                log(LogLevel::SECURITY, oss.str());
            }
            else
            {
                ostringstream oss;
                oss << "Client fd " << client_fd << " disconnected or framing error.";
                log(LogLevel::INFO, oss.str());
            }

            break;
        }

        {
            ostringstream oss;
            oss << "Received: " << message;
            log(LogLevel::INFO, oss.str());
        }

        bool send_failed = false;
        int failed_fd = -1;

        {
            lock_guard<mutex> lock(clients_mutex);

            // Look up the sender's username under the same lock that guards
            // client_fds, so it can't be erased out from under us mid-broadcast.
            string formatted_message = usernames[client_fd] + ": " + message;

            for (int fd : client_fds)
            {
                if (fd == client_fd)
                {
                    continue;
                }

                // now using send_message() so the broadcast is also framed correctly
                bool sent_ok = send_message(fd, MessageType::CHAT, formatted_message);

                if (!sent_ok)
                {
                    send_failed = true;
                    failed_fd = fd;
                }
            }
        }

        if (send_failed)
        {
            ostringstream oss;
            oss << "send_message failed for client " << failed_fd;
            log(LogLevel::WARN, oss.str());
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

        // Same "who's connected" state as client_fds, so it gets cleaned up
        // here too, under the same lock.
        usernames.erase(client_fd);
    }

    close(client_fd);
}

int main()
{
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd == -1)
    {
        ostringstream oss;
        oss << "Error: " << strerror(errno);
        log(LogLevel::ERROR, oss.str());
        return 1;
    }

    {
        log(LogLevel::INFO, "Socket created successfully!");

        ostringstream oss;
        oss << "File descriptor: " << server_fd;
        log(LogLevel::INFO, oss.str());
    }

    // SO_REUSEADDR lets us rebind to this port immediately after a restart,
    // instead of getting "Address already in use" while the OS holds the old
    // socket in TIME_WAIT. opt=1 means "enable this option".
    int opt = 1;

    // SOL_SOCKET = "this option operates at the general socket level" (as
    // opposed to a TCP-specific or IP-specific level, like IPPROTO_TCP would be)
    int result = setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Unlike bind/listen/accept failing, a failed setsockopt() here doesn't
    // break the server's ability to run right now - it can still bind and
    // serve clients just fine on this run. It only means a *future* restart
    // might hit the TIME_WAIT problem again. That's a WARN, not an ERROR:
    // something failed but the server keeps running fine.
    if (result == -1)
    {
        ostringstream oss;
        oss << "setsockopt(SO_REUSEADDR) failed: " << strerror(errno);
        log(LogLevel::WARN, oss.str());
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
    // the bind function expects a pointer struck to serveraddr because the bind function must be able to work with all
    // kinds of address families ,
    // so the general pointer of sockaddr is expected by the function
    result = bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));

    // each and every thread have unique errno
    // and the function running in that thread can read or set that threads content
    if (result == -1)
    {
        ostringstream oss;
        oss << "Bind failed: " << strerror(errno);
        log(LogLevel::ERROR, oss.str());
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
        ostringstream oss;
        oss << "Listen failed: " << strerror(errno);
        log(LogLevel::ERROR, oss.str());
        close(server_fd);
        return 1;
    }

    {
        log(LogLevel::INFO, "The server is listening");
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0; // deliberately NOT setting SA_RESTART, so accept() returns EINTR instead of silently retrying
    sigaction(SIGINT, &sa, nullptr);

    // loop condition now checks shutdown_flag so Ctrl+C can actually end the loop,
    // instead of while(true) which had no way to exit gracefully
    while (!shutdown_flag)
    {
        sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);

        /*we are assigning a file descriptor to a client conenction request
        By default now the accept function is of the blocking type
        i.e it blocks the calling thread till connection is found */
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);

        if (client_fd == -1)
        {
            // EINTR means a signal (like SIGINT) interrupted accept() while it was blocking -
            // this is not a real error, it's expected during shutdown, so don't print
            // "Accept failed" for it. Just go back to the top of the loop, where
            // while(!shutdown_flag) will now correctly evaluate to false and exit.
            if (errno == EINTR)
            {
                continue;
            }

            ostringstream oss;
            oss << "Accept failed: " << strerror(errno);
            log(LogLevel::ERROR, oss.str());
            continue;
        }

        {
            log(LogLevel::INFO, "Client connected!");

            ostringstream oss;
            oss << "Client file descriptor: " << client_fd;
            log(LogLevel::INFO, oss.str());
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

    // shutdown_flag became true so we will clean up before exiting
    {
        log(LogLevel::INFO, "Shutdown signal received. Closing all client connections...");
    }

    {
        lock_guard<mutex> lock(clients_mutex);
        for (int fd : client_fds)
        {
            close(fd);
        }
    }

    close(server_fd);

    {
        log(LogLevel::INFO, "Server shut down gracefully.");
    }

    return 0;
}