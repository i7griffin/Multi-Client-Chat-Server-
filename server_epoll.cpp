
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <fcntl.h>
#include <cstring>

// header for sockaddr_in and htons
#include <netinet/in.h>
#include <arpa/inet.h>

#include <csignal>
#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <map>
#include <vector>
#include <deque>
#include <cstdint>
#include <sys/epoll.h>

using namespace std;

constexpr int PORT = 8080;
constexpr int MAX_EVENTS = 100;

// atomic bool is used for shutdown is to ask the compile to check for other way to check the modified value of the flag and not to check only in the flow of program
atomic<bool> shutdown_flag(false);

// signal handler is what we use to change the flag value outside of the program execution flow
void signal_handler(int signum) {
    shutdown_flag = true;
}

/* In the epoll version, only the main event-loop thread
   accesses the clients map. So we do not need clients_mutex
   for protecting client state from multiple client threads. */
map<int, struct Client> clients;

// Maps a connected client's fd to the username they registered at connect
// time. This is the same conceptual "who's connected" state as client_fds,
// just richer, so it's stored in each Client object.
const size_t MAX_MESSAGE_SIZE = 1024;

// Log levels, ordered roughly by how much attention they deserve.
// INFO      - normal, expected lifecycle events (connect, disconnect, startup)
// WARN      - something failed but the server keeps running fine (a broadcast send failed)
// ERROR     - a syscall failed in a way that affects server operation (bind/listen/accept failed)
// SECURITY  - a client did something that looks like probing/attacking the protocol
//             (claiming an oversized message length), as opposed to just disconnecting
enum class LogLevel {
    INFO,
    WARN,
    ERROR,
    SECURITY
};

string level_to_string(LogLevel level) {
    switch (level) {
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
void log(LogLevel level, const string& message) {
    auto now = chrono::system_clock::now();
    time_t now_time_t = chrono::system_clock::to_time_t(now);

    // localtime() is not thread-safe on its own (it can return a pointer to
    // shared static storage), so we use localtime_r, which fills a
    // caller-provided struct instead.
    tm local_tm{};
    localtime_r(&now_time_t, &local_tm);

    // ostringstream is a tool for constructing strings using << instead of repeatedly using string concatenation.
    ostringstream timestamp_stream;
    timestamp_stream << put_time(&local_tm, "%Y-%m-%d %H:%M:%S");

    cout << "[" << timestamp_stream.str() << "] "
         << "[" << level_to_string(level) << "] "
         << message << endl;
}

// The type byte lets receivers tell "this is the one-time username handshake"
// apart from "this is a regular chat message" without needing a second,
// differently-shaped protocol. It's a single byte, so it has no endianness
// to worry about (endianness only matters for multi-byte values like our
// 4-byte length header).
enum class MessageType : uint8_t {
    USERNAME = 0,
    CHAT = 1
};


/*
    Protocol:
    [1 byte message type]
    [4 byte message length]
    [message payload]
*/


/*this is the structure of client for all clients joiniing the server */
struct Client {
    int fd;

    string username;

    // Stores bytes received from the client.
    // A message may arrive in multiple recv() calls.
    vector<char> input_buffer;

    // Stores messages waiting to be sent to this client.
    deque<string> output_queue;

    // Tracks how many bytes of the first queued packet were sent.
    size_t output_offset = 0;

    bool registered = false;
    bool alive = true;
};


bool set_nonblocking(int fd);

bool update_epoll_interest(int epoll_fd, Client& client);

void queue_message(
    Client& client,
    MessageType type,
    const string& message
);

bool parse_messages(int epoll_fd, Client& client);

bool handle_read(int epoll_fd, Client& client);

bool handle_write(int epoll_fd, Client& client);

void broadcast_message(
    int epoll_fd,
    int sender_fd,
    const string& message
);

void remove_client(int epoll_fd, int client_fd);



bool set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);

    if (flags == -1) {
        return false;
    }

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        return false;
    }

    return true;
}




bool update_epoll_interest(
    int epoll_fd,
    Client& client
) {
    epoll_event event{};

    event.data.fd = client.fd;

    // EPOLLIN means we want to know when data can be read.
    // EPOLLRDHUP helps detect when the peer closes its write side.
    event.events = EPOLLIN | EPOLLRDHUP;

    // EPOLLOUT is enabled only when there is data waiting to be sent.
    // Otherwise, writable sockets could cause unnecessary event notifications.
    if (!client.output_queue.empty()) {
        event.events |= EPOLLOUT;
    }

    return epoll_ctl( epoll_fd, EPOLL_CTL_MOD, client.fd, &event ) != -1;
}


void queue_message(
    Client& client,
    MessageType type,
    const string& message
) {
    // The type byte lets the receiver distinguish USERNAME and CHAT.
    uint8_t type_byte = static_cast<uint8_t>(type);

    uint32_t message_length =
        static_cast<uint32_t>(message.size());

    // htonl means host to network long.
    // We convert the 32-bit message length to network byte order.
    uint32_t network_length = htonl(message_length);

    string packet;

    packet.reserve(
        sizeof(type_byte) +
        sizeof(network_length) +
        message.size()
    );

    packet.append(
        reinterpret_cast<const char*>(&type_byte),
        sizeof(type_byte)
    );

    packet.append(
        reinterpret_cast<const char*>(&network_length),
        sizeof(network_length)
    );

    packet.append(message);

    // We queue the complete packet instead of sending it immediately.
    // A nonblocking send() may send only part of the packet.
    client.output_queue.push_back(move(packet));
}


/*this function os present to remove the client*/

void remove_client(
    int epoll_fd,
    int client_fd
) {
    auto it = clients.find(client_fd);

    if (it == clients.end()) {
        return;
    }

    string username = it->second.username;

    // Remove the fd from the epoll interest list.
    epoll_ctl(epoll_fd,EPOLL_CTL_DEL,client_fd,nullptr);

    close(client_fd);

    // Remove the client's stored username and buffers too.
    clients.erase(it);

    ostringstream oss;

    oss << "Client fd " << client_fd
        << " (" << username << ") disconnected.";

    log(LogLevel::INFO, oss.str());
}


/*This function is present to broadcat message to all the users*/
void broadcast_message(
    int epoll_fd,
    int sender_fd,
    const string& message
) {
    auto sender_it = clients.find(sender_fd);

    if (sender_it == clients.end()) {
        return;
    }

    string formatted_message =
        sender_it->second.username + ": " + message;

    for (auto& [fd, client] : clients) {
        if (fd == sender_fd || !client.registered) {
            continue;
        }

        // Queue the broadcast for each connected client.
        queue_message(
            client,
            MessageType::CHAT,
            formatted_message
        );

        // Register EPOLLOUT so we know when the socket
        // can send the queued message.
        if (!update_epoll_interest(epoll_fd, client)) {
            client.alive = false;
        }
    }
}


bool parse_messages(
    int epoll_fd,
    Client& client
) {
    constexpr size_t HEADER_SIZE =
        sizeof(uint8_t) + sizeof(uint32_t);

    while (true) {
        // We need the type byte and 4-byte length first.
        if (client.input_buffer.size() < HEADER_SIZE) {
            return true;
        }

        uint8_t type_byte;

        memcpy(
            &type_byte,
            client.input_buffer.data(),
            sizeof(type_byte)
        );

        uint32_t network_length;

        memcpy(
            &network_length,
            client.input_buffer.data() + sizeof(type_byte),
            sizeof(network_length)
        );

        // ntohl means network to host.
        uint32_t message_length = ntohl(network_length);

        // Reject oversized message lengths.
        if (message_length > MAX_MESSAGE_SIZE) {
            ostringstream oss;

            oss << "Client fd " << client.fd
                << " claimed an oversized message length (> "
                << MAX_MESSAGE_SIZE << " bytes).";

            log(LogLevel::SECURITY, oss.str());

            return false;
        }

        size_t total_length =
            HEADER_SIZE + static_cast<size_t>(message_length);

        // The header arrived, but the complete payload has not arrived yet.
        // Keep the bytes and wait for another EPOLLIN event.
        if (client.input_buffer.size() < total_length) {
            return true;
        }

        string message(
            client.input_buffer.data() + HEADER_SIZE,
            message_length
        );

        MessageType type =
            static_cast<MessageType>(type_byte);

        //checking if the client sent the username first 
        if (!client.registered) {
            if (type != MessageType::USERNAME) {
                log(
                    LogLevel::WARN,
                    "Client sent CHAT before USERNAME. Closing connection."
                );

                return false;
            }

            if (message.empty()) {
                log(
                    LogLevel::WARN,
                    "Client sent an empty username. Closing connection."
                );

                return false;
            }

            client.username = message;
            client.registered = true;

            log(
                LogLevel::INFO,
                "Client registered as \"" +
                client.username + "\"."
            );
        }

        //this is the block for normal chat message

        else {
            if (type != MessageType::CHAT) {
                log(
                    LogLevel::WARN,
                    "Unexpected message type. Closing connection."
                );

                return false;
            }

            log(
                LogLevel::INFO,
                "Received: " + message
            );

            broadcast_message(
                epoll_fd,
                client.fd,
                message
            );
        }

        // Remove the complete message from the input buffer.
        // Any remaining bytes may contain another complete message.
        client.input_buffer.erase(
            client.input_buffer.begin(),
            client.input_buffer.begin() + total_length
        );
    }
}



bool handle_read(
    int epoll_fd,
    Client& client
) {
    char buffer[4096];

    while (true) {
        ssize_t result = recv(
            client.fd,
            buffer,
            sizeof(buffer),
            0
        );

        if (result > 0) {
            client.input_buffer.insert(
                client.input_buffer.end(),
                buffer,
                buffer + result
            );

            // Parse any complete messages stored in the buffer.
            if (!parse_messages(epoll_fd, client)) {
                return false;
            }
        }

        else if (result == 0) {
            // recv() returning 0 means the peer performed
            // an orderly shutdown of its sending side.
            return false;
        }

        else {
            if (errno == EINTR) {
                continue;
            }

            if (
                errno == EAGAIN ||
                errno == EWOULDBLOCK
            ) {
                // No more data is available right now.
                // Return to epoll_wait() and wait for another event.
                return true;
            }

            perror("recv");
            return false;
        }
    }
}



bool handle_write(
    int epoll_fd,
    Client& client
) {
    while (!client.output_queue.empty()) {
        string& packet = client.output_queue.front();

        const char* data =
            packet.data() + client.output_offset;

        size_t remaining =
            packet.size() - client.output_offset;

        ssize_t result = send(
            client.fd,
            data,
            remaining,
            MSG_NOSIGNAL
        );

        if (result > 0) {
            client.output_offset +=
                static_cast<size_t>(result);

            if (
                client.output_offset ==
                packet.size()
            ) {
                client.output_queue.pop_front();

                client.output_offset = 0;
            }
        }

        else if (result == -1) {
            if (errno == EINTR) {
                continue;
            }

            if (
                errno == EAGAIN ||
                errno == EWOULDBLOCK
            ) {
                // The socket's send buffer is full.
                // Wait for another EPOLLOUT event.
                break;
            }

            perror("send");
            return false;
        }

        else {
            return false;
        }
    }

    // Update the events:
    // If output_queue is empty, EPOLLOUT is removed.
    return update_epoll_interest(
        epoll_fd,
        client
    );
}



void accept_clients(
    int epoll_fd,
    int server_fd
) {
    while (true) {
        sockaddr_in client_addr{};

        socklen_t client_addr_len =
            sizeof(client_addr);

        // Since server_fd is nonblocking, accept()
        // returns immediately if no connection is waiting.
        int client_fd = accept(
            server_fd,
            reinterpret_cast<sockaddr*>(&client_addr),
            &client_addr_len
        );

        if (client_fd == -1) {
            if (errno == EINTR) {
                continue;
            }

            if (
                errno == EAGAIN ||
                errno == EWOULDBLOCK
            ) {
                // All pending connections have been accepted.
                break;
            }

            perror("accept");
            break;
        }

        // Make the accepted client socket nonblocking too.
        if (!set_nonblocking(client_fd)) {
            perror("fcntl client");
            close(client_fd);
            continue;
        }

        Client client{};

        client.fd = client_fd;

        // Register the new client socket with epoll.
        epoll_event client_event{};

        client_event.data.fd = client_fd;

        client_event.events =
            EPOLLIN | EPOLLRDHUP;

        if (epoll_ctl(
            epoll_fd,
            EPOLL_CTL_ADD,
            client_fd,
            &client_event
        ) == -1) {
            perror("epoll_ctl client");

            close(client_fd);
            continue;
        }

        clients.emplace(
            client_fd,
            move(client)
        );

        ostringstream oss;

        oss << "New client connected. File descriptor: "
            << client_fd;

        log(LogLevel::INFO, oss.str());
    }
}



int main() {
   

    struct sigaction sa{};

    sa.sa_handler = signal_handler;

    sigemptyset(&sa.sa_mask);

    // Deliberately not setting SA_RESTART.
    sa.sa_flags = 0;

    if (sigaction(SIGINT, &sa, nullptr) == -1) {
        perror("sigaction");
        return 1;
    }


    int server_fd = socket( AF_INET, SOCK_STREAM, 0
    );

    if (server_fd == -1) {
        ostringstream oss;

        oss << "Socket failed: " << strerror(errno);

        log(LogLevel::ERROR, oss.str());

        return 1;
    }

    log(
        LogLevel::INFO,
        "Socket created successfully!"
    );

    // SO_REUSEADDR lets us rebind to this port immediately after a restart,
    // instead of getting "Address already in use" while the OS holds the old
    // socket in TIME_WAIT. opt=1 means "enable this option".
    int opt = 1;

    int result = setsockopt(
        server_fd,
        SOL_SOCKET,
        SO_REUSEADDR,
        &opt,
        sizeof(opt)
    );

    if (result == -1) {
        ostringstream oss;

        oss << "setsockopt(SO_REUSEADDR) failed: "
            << strerror(errno);

        log(LogLevel::WARN, oss.str());
    }



    sockaddr_in server_addr{};

    memset(
        &server_addr,
        0,
        sizeof(server_addr)
    );

    // To listen through all available local interfaces.
    server_addr.sin_addr.s_addr = INADDR_ANY;

    server_addr.sin_family = AF_INET;

    server_addr.sin_port = htons(PORT);


    result = bind(
        server_fd,
        reinterpret_cast<sockaddr*>(&server_addr),
        sizeof(server_addr)
    );

    if (result == -1) {
        ostringstream oss;

        oss << "Bind failed: " << strerror(errno);

        log(LogLevel::ERROR, oss.str());

        close(server_fd);
        return 1;
    }


    // fcntl receives the flags of the socket now.
    // O_NONBLOCK is ORed with the existing flags.
    if (!set_nonblocking(server_fd)) {
        ostringstream oss;

        oss << "fcntl failed: " << strerror(errno);

        log(LogLevel::ERROR, oss.str());

        close(server_fd);
        return 1;
    }


    result = listen(server_fd, 5);

    if (result == -1) {
        ostringstream oss;

        oss << "Listen failed: " << strerror(errno);

        log(LogLevel::ERROR, oss.str());

        close(server_fd);
        return 1;
    }

    log(
        LogLevel::INFO,
        "The server is listening."
    );



    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);

    // epoll_create1() returns a file descriptor representing the epoll instance.
    // epoll instances are fds too, same as sockets.
    // EPOLL_CLOEXEC means the fd will be closed if the process executes another program.
    if (epoll_fd == -1) {
        int saved_errno = errno;

        ostringstream oss;

        oss << "Failed to create epoll instance: "
            << strerror(saved_errno);

        log(LogLevel::ERROR, oss.str());

        close(server_fd);
        return 1;
    }


    //registering server socket in epoll
    epoll_event server_event{};

    server_event.data.fd = server_fd;

    // EPOLLIN means the listening socket has pending connections.
    server_event.events = EPOLLIN;

    if (epoll_ctl(
        epoll_fd,
        EPOLL_CTL_ADD,
        server_fd,
        &server_event
    ) == -1) {
        perror("epoll_ctl server");

        close(epoll_fd);
        close(server_fd);

        return 1;
    }

    log(
        LogLevel::INFO,
        "Server registered with epoll."
    );


    epoll_event events[MAX_EVENTS];

    while (!shutdown_flag) {
        // epoll_wait() blocks until one or more registered fds
        // become ready, or until a signal interrupts it.
        int num_ready = epoll_wait(
            epoll_fd,
            events,
            MAX_EVENTS,
            -1
        );

        if (num_ready == -1) {
            if (errno == EINTR) {
                // A signal interrupted epoll_wait().
                // Check shutdown_flag at the loop condition.
                continue;
            }

            perror("epoll_wait");
            break;
        }

        // epoll_wait() returns multiple ready events.
        // We must process every event returned in this batch.
        for (int i = 0; i < num_ready; i++) {
            int ready_fd = events[i].data.fd;

            uint32_t flags = events[i].events;


            if (ready_fd == server_fd) {
                // A connection is waiting.
                // Accept pending connections until EAGAIN.
                accept_clients(
                    epoll_fd,
                    server_fd
                );

                continue;
            }


            auto it = clients.find(ready_fd);

            if (it == clients.end()) {
                continue;
            }

            Client& client = it->second;

            // Read available data first.
            if (flags & EPOLLIN) {
                if (!handle_read(epoll_fd, client)) {
                    client.alive = false;
                }
            }

            // Handle errors and disconnect notifications.
            if (
                flags &
                (EPOLLERR | EPOLLHUP | EPOLLRDHUP)
            ) {
                client.alive = false;
            }

            // Send queued messages when the socket is writable.
            if (
                client.alive &&
                (flags & EPOLLOUT)
            ) {
                if (!handle_write(epoll_fd, client)) {
                    client.alive = false;
                }
            }

    

            if (!client.alive) {
                remove_client(
                    epoll_fd,
                    ready_fd
                );
            }
        }
    }



    log(
        LogLevel::INFO,
        "Shutdown signal received. Closing all client connections..."
    );

    for (auto& [fd, client] : clients) {
        epoll_ctl(
            epoll_fd,
            EPOLL_CTL_DEL,
            fd,
            nullptr
        );

        close(fd);
    }

    clients.clear();

    close(server_fd);
    close(epoll_fd);

    log(
        LogLevel::INFO,
        "Server shut down gracefully."
    );

    return 0;
}