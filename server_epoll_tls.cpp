// server_epoll_tls.cpp
// Your epoll chat server, now speaking TLS 1.3 (OpenSSL) on top of the same
// non-blocking, single-threaded event loop and the same wire protocol:
//   [1 byte type][4 byte length][payload]   -- but now inside the TLS tunnel.
//
// Build:  g++ -std=c++17 -O2 -Wall -Wextra server_epoll_tls.cpp -o server_tls -lssl -lcrypto
// Run:    ./server_tls [cert.pem] [key.pem]      (defaults: certs/server.crt certs/server.key)

#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <fcntl.h>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <csignal>
#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <map>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <sys/epoll.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

using namespace std;

constexpr int PORT = 8080;
constexpr int MAX_EVENTS = 128;
const size_t MAX_MESSAGE_SIZE = 1024;

// One TLS record carries at most 16 KB of plaintext, so we never hand
// SSL_write() more than that at once.
constexpr size_t TLS_CHUNK = 16 * 1024;

// A client that does not drain its socket must not be allowed to make us
// buffer unbounded data (slow-consumer DoS).
constexpr size_t MAX_OUTPUT_BUFFER = 1024 * 1024;

// A peer that connects but never finishes the TLS handshake / never sends a
// username would otherwise hold a slot forever (slowloris-style).
constexpr int HANDSHAKE_TIMEOUT_SEC = 10;

atomic<bool> shutdown_flag(false);
void signal_handler(int) { shutdown_flag = true; }

// ---------------------------------------------------------------- logging
enum class LogLevel { INFO, WARN, ERROR, SECURITY };

string level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::INFO:     return "INFO";
        case LogLevel::WARN:     return "WARN";
        case LogLevel::ERROR:    return "ERROR";
        case LogLevel::SECURITY: return "SECURITY";
    }
    return "UNKNOWN";
}

void log(LogLevel level, const string& message) {
    auto now = chrono::system_clock::now();
    time_t t = chrono::system_clock::to_time_t(now);
    tm local_tm{};
    localtime_r(&t, &local_tm);
    ostringstream ts;
    ts << put_time(&local_tm, "%Y-%m-%d %H:%M:%S");
    cout << "[" << ts.str() << "] [" << level_to_string(level) << "] " << message << endl;
}

// Drains OpenSSL's per-thread error queue into the log. Must be called after a
// failed SSL_* call, otherwise stale errors confuse the next SSL_get_error().
void log_ssl_errors(LogLevel level, const string& context) {
    unsigned long e;
    while ((e = ERR_get_error()) != 0) {
        char buf[256];
        ERR_error_string_n(e, buf, sizeof(buf));
        log(level, context + ": " + buf);
    }
}

// ---------------------------------------------------------------- protocol
enum class MessageType : uint8_t { USERNAME = 0, CHAT = 1 };

enum class TlsState { HANDSHAKE, ESTABLISHED };

struct Client {
    int fd = -1;
    string username;

    // ---- TLS ----
    SSL* ssl = nullptr;
    TlsState tls_state = TlsState::HANDSHAKE;
    // True when the last SSL_* call needed the socket to become WRITABLE
    // (handshake flight, or a write the kernel buffer could not take yet).
    bool want_epollout = false;
    // True when a pending SSL_write is waiting for the socket to become
    // READABLE (TLS needs to read before it can finish writing).
    bool write_wants_read = false;

    // ---- buffering ----
    vector<char> input_buffer;
    // ONE contiguous outgoing buffer instead of a deque of packets: all
    // messages queued in the same epoll iteration are coalesced into a
    // single SSL_write -> fewer TLS records, fewer syscalls, less overhead.
    string output_buffer;
    size_t output_offset = 0;
    // OpenSSL requires a retried SSL_write() to use the same length as the
    // call that returned WANT_READ/WANT_WRITE. 0 = no retry pending.
    size_t pending_write_len = 0;

    uint32_t epoll_events = 0;   // cached, so we skip redundant epoll_ctl calls
    chrono::steady_clock::time_point connected_at;

    bool registered = false;
    bool alive = true;
};

map<int, Client> clients;
SSL_CTX* g_ssl_ctx = nullptr;

// fds of OTHER clients that broadcast_message() decided to drop (e.g. slow consumers).
// They are removed after the current batch of epoll events, never mid-iteration.
vector<int> dead_fds;

// ---------------------------------------------------------------- epoll helpers
bool update_epoll_interest(int epoll_fd, Client& c) {
    uint32_t events = EPOLLIN | EPOLLRDHUP;

    bool has_output = c.output_offset < c.output_buffer.size();
    if (c.want_epollout || (c.tls_state == TlsState::ESTABLISHED && has_output && !c.write_wants_read)) {
        events |= EPOLLOUT;
    }

    if (events == c.epoll_events) return true;   // nothing changed: no syscall

    epoll_event ev{};
    ev.data.fd = c.fd;
    ev.events = events;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, c.fd, &ev) == -1) return false;
    c.epoll_events = events;
    return true;
}

// ---------------------------------------------------------------- outgoing data
bool queue_message(Client& c, MessageType type, const string& message) {
    uint8_t type_byte = static_cast<uint8_t>(type);
    uint32_t network_length = htonl(static_cast<uint32_t>(message.size()));

    // Compact already-sent bytes before growing, but only when no SSL_write
    // retry is pending (a pending retry must see identical data).
    if (c.pending_write_len == 0 && c.output_offset > 0 && c.output_offset == c.output_buffer.size()) {
        c.output_buffer.clear();
        c.output_offset = 0;
    }

    c.output_buffer.append(reinterpret_cast<const char*>(&type_byte), sizeof(type_byte));
    c.output_buffer.append(reinterpret_cast<const char*>(&network_length), sizeof(network_length));
    c.output_buffer.append(message);

    return (c.output_buffer.size() - c.output_offset) <= MAX_OUTPUT_BUFFER;
}

// Pushes as much of the output buffer through TLS as the socket accepts.
// Returns false if the connection must be dropped.
bool flush_output(Client& c) {
    c.write_wants_read = false;
    c.want_epollout = false;

    while (c.output_offset < c.output_buffer.size()) {
        size_t remaining = c.output_buffer.size() - c.output_offset;
        size_t n = c.pending_write_len ? c.pending_write_len : min(remaining, TLS_CHUNK);

        ERR_clear_error();
        int r = SSL_write(c.ssl, c.output_buffer.data() + c.output_offset, static_cast<int>(n));
        if (r > 0) {
            c.output_offset += static_cast<size_t>(r);
            c.pending_write_len = 0;
            continue;
        }

        int err = SSL_get_error(c.ssl, r);
        if (err == SSL_ERROR_WANT_WRITE) {
            c.pending_write_len = n;
            c.want_epollout = true;      // kernel send buffer full: wait for EPOLLOUT
            return true;
        }
        if (err == SSL_ERROR_WANT_READ) {
            c.pending_write_len = n;
            c.write_wants_read = true;   // TLS must read first; EPOLLIN is always armed
            return true;
        }
        if (err == SSL_ERROR_ZERO_RETURN) return false;
        log_ssl_errors(LogLevel::WARN, "SSL_write failed on fd " + to_string(c.fd));
        return false;
    }

    // Everything sent.
    c.output_buffer.clear();
    c.output_offset = 0;
    return true;
}

// ---------------------------------------------------------------- client lifecycle
void remove_client(int epoll_fd, int client_fd) {
    auto it = clients.find(client_fd);
    if (it == clients.end()) return;

    Client& c = it->second;
    string username = c.username;

    if (c.ssl) {
        // Best-effort close_notify. We never wait for the peer's reply.
        if (c.tls_state == TlsState::ESTABLISHED) {
            ERR_clear_error();
            SSL_shutdown(c.ssl);
        }
        SSL_free(c.ssl);   // does not close the fd (we own it)
        c.ssl = nullptr;
    }

    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
    close(client_fd);
    clients.erase(it);

    ostringstream oss;
    oss << "Client fd " << client_fd << " (" << username << ") disconnected.";
    log(LogLevel::INFO, oss.str());
}

void broadcast_message(int epoll_fd, int sender_fd, const string& message) {
    auto sender_it = clients.find(sender_fd);
    if (sender_it == clients.end()) return;

    string formatted = sender_it->second.username + ": " + message;

    for (auto& [fd, client] : clients) {
        if (fd == sender_fd || !client.registered) continue;

        if (!queue_message(client, MessageType::CHAT, formatted)) {
            log(LogLevel::WARN, "fd " + to_string(fd) + " is not draining its socket; dropping.");
            client.alive = false;
            dead_fds.push_back(fd);
            continue;
        }
        if (!update_epoll_interest(epoll_fd, client)) {
            client.alive = false;
            dead_fds.push_back(fd);
        }
    }
}

bool parse_messages(int epoll_fd, Client& client) {
    constexpr size_t HEADER_SIZE = sizeof(uint8_t) + sizeof(uint32_t);

    // Parse everything we have, then erase the consumed prefix ONCE
    // (the old code erased after every single message).
    size_t pos = 0;
    bool ok = true;

    while (true) {
        size_t avail = client.input_buffer.size() - pos;
        if (avail < HEADER_SIZE) break;

        uint8_t type_byte;
        memcpy(&type_byte, client.input_buffer.data() + pos, sizeof(type_byte));

        uint32_t network_length;
        memcpy(&network_length, client.input_buffer.data() + pos + sizeof(type_byte), sizeof(network_length));
        uint32_t message_length = ntohl(network_length);

        if (message_length > MAX_MESSAGE_SIZE) {
            ostringstream oss;
            oss << "Client fd " << client.fd << " claimed an oversized message length (> "
                << MAX_MESSAGE_SIZE << " bytes).";
            log(LogLevel::SECURITY, oss.str());
            ok = false;
            break;
        }

        size_t total_length = HEADER_SIZE + message_length;
        if (avail < total_length) break;

        string message(client.input_buffer.data() + pos + HEADER_SIZE, message_length);
        MessageType type = static_cast<MessageType>(type_byte);

        if (!client.registered) {
            if (type != MessageType::USERNAME) {
                log(LogLevel::WARN, "Client sent CHAT before USERNAME. Closing connection.");
                ok = false;
                break;
            }
            if (message.empty()) {
                log(LogLevel::WARN, "Client sent an empty username. Closing connection.");
                ok = false;
                break;
            }
            client.username = message;
            client.registered = true;
            log(LogLevel::INFO, "Client registered as \"" + client.username + "\".");
        } else {
            if (type != MessageType::CHAT) {
                log(LogLevel::WARN, "Unexpected message type. Closing connection.");
                ok = false;
                break;
            }
            log(LogLevel::INFO, "Received: " + message);
            broadcast_message(epoll_fd, client.fd, message);
        }

        pos += total_length;
    }

    if (pos > 0) {
        client.input_buffer.erase(client.input_buffer.begin(), client.input_buffer.begin() + pos);
    }
    return ok;
}

// ---------------------------------------------------------------- TLS handshake / read
// Drives the non-blocking handshake. Returns false if the connection must be dropped.
bool do_handshake(Client& c) {
    ERR_clear_error();
    int r = SSL_accept(c.ssl);
    if (r == 1) {
        c.tls_state = TlsState::ESTABLISHED;
        c.want_epollout = false;
        ostringstream oss;
        oss << "TLS handshake complete on fd " << c.fd << " (" << SSL_get_version(c.ssl)
            << ", " << SSL_get_cipher_name(c.ssl) << ")";
        log(LogLevel::INFO, oss.str());
        return true;
    }

    int err = SSL_get_error(c.ssl, r);
    if (err == SSL_ERROR_WANT_READ)  { c.want_epollout = false; return true; }
    if (err == SSL_ERROR_WANT_WRITE) { c.want_epollout = true;  return true; }

    // Anything else: a plaintext client hitting the TLS port, a client that only
    // speaks TLS 1.2, a bad ClientHello, a scanner... worth a SECURITY log line.
    log_ssl_errors(LogLevel::SECURITY, "TLS handshake failed on fd " + to_string(c.fd));
    return false;
}

bool handle_read(int epoll_fd, Client& c) {
    char buffer[16 * 1024];

    // Level-triggered epoll only knows about the SOCKET, not about decrypted
    // bytes OpenSSL may already hold internally. So we must keep calling
    // SSL_read() until it reports WANT_READ, otherwise data could sit unseen.
    while (true) {
        ERR_clear_error();
        int r = SSL_read(c.ssl, buffer, sizeof(buffer));
        if (r > 0) {
            c.input_buffer.insert(c.input_buffer.end(), buffer, buffer + r);
            if (!parse_messages(epoll_fd, c)) return false;
            if (!c.alive) return false;
            continue;
        }

        int err = SSL_get_error(c.ssl, r);
        switch (err) {
            case SSL_ERROR_WANT_READ:
                return true;
            case SSL_ERROR_WANT_WRITE:
                c.want_epollout = true;   // TLS needs to flush something (e.g. key update)
                return true;
            case SSL_ERROR_ZERO_RETURN:   // clean close_notify
                return false;
            default:
                log_ssl_errors(LogLevel::WARN, "SSL_read failed on fd " + to_string(c.fd));
                return false;
        }
    }
}

// One entry point for every epoll event on a client socket.
bool handle_io(int epoll_fd, Client& c, uint32_t flags) {
    bool read_now = flags & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR);

    if (c.tls_state == TlsState::HANDSHAKE) {
        if (!do_handshake(c)) return false;
        if (c.tls_state == TlsState::HANDSHAKE) return update_epoll_interest(epoll_fd, c);
        // The client's first application bytes may already be inside OpenSSL's
        // buffer, so attempt a read right away.
        read_now = true;
    }

    if (read_now && !handle_read(epoll_fd, c)) return false;
    if (!flush_output(c)) return false;
    return update_epoll_interest(epoll_fd, c);
}

// ---------------------------------------------------------------- sockets
void accept_clients(int epoll_fd, int server_fd) {
    while (true) {
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);

        // accept4 gives us a non-blocking, close-on-exec socket in ONE syscall.
        int client_fd = accept4(server_fd, reinterpret_cast<sockaddr*>(&addr), &len,
                                SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd == -1) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            perror("accept4");
            break;
        }

        // Chat messages are small and latency-sensitive; don't let Nagle batch them.
        int one = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        SSL* ssl = SSL_new(g_ssl_ctx);
        if (!ssl || SSL_set_fd(ssl, client_fd) != 1) {
            log_ssl_errors(LogLevel::ERROR, "SSL_new/SSL_set_fd failed");
            if (ssl) SSL_free(ssl);
            close(client_fd);
            continue;
        }
        SSL_set_accept_state(ssl);

        Client client;
        client.fd = client_fd;
        client.ssl = ssl;
        client.connected_at = chrono::steady_clock::now();
        client.epoll_events = EPOLLIN | EPOLLRDHUP;

        epoll_event ev{};
        ev.data.fd = client_fd;
        ev.events = client.epoll_events;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) == -1) {
            perror("epoll_ctl client");
            SSL_free(ssl);
            close(client_fd);
            continue;
        }

        clients.emplace(client_fd, move(client));

        char ip[INET_ADDRSTRLEN] = "?";
        inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
        ostringstream oss;
        oss << "New TCP connection from " << ip << ":" << ntohs(addr.sin_port)
            << " (fd " << client_fd << "), starting TLS handshake.";
        log(LogLevel::INFO, oss.str());
    }
}

// Drops clients that sit in the handshake / pre-username state for too long.
void sweep_stale_clients(int epoll_fd) {
    auto now = chrono::steady_clock::now();
    vector<int> stale;
    for (auto& [fd, c] : clients) {
        if (c.registered) continue;
        if (chrono::duration_cast<chrono::seconds>(now - c.connected_at).count() >= HANDSHAKE_TIMEOUT_SEC) {
            stale.push_back(fd);
        }
    }
    for (int fd : stale) {
        log(LogLevel::SECURITY, "fd " + to_string(fd) + " did not complete handshake/registration in time.");
        remove_client(epoll_fd, fd);
    }
}

// ---------------------------------------------------------------- TLS context
SSL_CTX* create_ssl_context(const char* cert_file, const char* key_file) {
    SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) {
        log_ssl_errors(LogLevel::ERROR, "SSL_CTX_new");
        return nullptr;
    }

    // TLS 1.3 only: 1-RTT handshake, forward secrecy always, no legacy
    // ciphers/renegotiation/compression. If you must support old clients,
    // change this to TLS1_2_VERSION.
    SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);

    SSL_CTX_set_options(ctx,
        SSL_OP_NO_COMPRESSION |            // CRIME-style attacks
        SSL_OP_NO_RENEGOTIATION |
        SSL_OP_CIPHER_SERVER_PREFERENCE |
        SSL_OP_IGNORE_UNEXPECTED_EOF);     // peer closing without close_notify = normal disconnect

    SSL_CTX_set_mode(ctx,
        SSL_MODE_ENABLE_PARTIAL_WRITE |         // SSL_write may return after a partial send (non-blocking)
        SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER |   // our output buffer can reallocate between retries
        SSL_MODE_RELEASE_BUFFERS);              // free per-connection I/O buffers while idle -> far less RAM per client

    // AES-128-GCM first (hardware accelerated via AES-NI, still 128-bit secure),
    // ChaCha20 for CPUs without AES acceleration.
    SSL_CTX_set_ciphersuites(ctx,
        "TLS_AES_128_GCM_SHA256:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_256_GCM_SHA384");
    SSL_CTX_set1_groups_list(ctx, "X25519:P-256");

    if (SSL_CTX_use_certificate_chain_file(ctx, cert_file) != 1) {
        log_ssl_errors(LogLevel::ERROR, string("Loading certificate ") + cert_file);
        SSL_CTX_free(ctx);
        return nullptr;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, key_file, SSL_FILETYPE_PEM) != 1) {
        log_ssl_errors(LogLevel::ERROR, string("Loading private key ") + key_file);
        SSL_CTX_free(ctx);
        return nullptr;
    }
    if (SSL_CTX_check_private_key(ctx) != 1) {
        log_ssl_errors(LogLevel::ERROR, "Private key does not match certificate");
        SSL_CTX_free(ctx);
        return nullptr;
    }
    return ctx;
}

// ---------------------------------------------------------------- main
int main(int argc, char** argv) {
    const char* cert_file = argc > 1 ? argv[1] : "certs/server.crt";
    const char* key_file  = argc > 2 ? argv[2] : "certs/server.key";

    // OpenSSL writes with plain write(), not send(MSG_NOSIGNAL). Writing to a
    // peer that already vanished would otherwise kill the whole server.
    signal(SIGPIPE, SIG_IGN);

    struct sigaction sa{};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGINT, &sa, nullptr) == -1 || sigaction(SIGTERM, &sa, nullptr) == -1) {
        perror("sigaction");
        return 1;
    }

    g_ssl_ctx = create_ssl_context(cert_file, key_file);
    if (!g_ssl_ctx) return 1;
    log(LogLevel::INFO, "TLS context ready (TLS 1.3 only).");

    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (server_fd == -1) {
        log(LogLevel::ERROR, string("Socket failed: ") + strerror(errno));
        return 1;
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        log(LogLevel::WARN, string("setsockopt(SO_REUSEADDR) failed: ") + strerror(errno));
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == -1) {
        log(LogLevel::ERROR, string("Bind failed: ") + strerror(errno));
        close(server_fd);
        return 1;
    }
    if (listen(server_fd, SOMAXCONN) == -1) {
        log(LogLevel::ERROR, string("Listen failed: ") + strerror(errno));
        close(server_fd);
        return 1;
    }
    log(LogLevel::INFO, "The server is listening (TLS) on port " + to_string(PORT) + ".");

    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd == -1) {
        log(LogLevel::ERROR, string("Failed to create epoll instance: ") + strerror(errno));
        close(server_fd);
        return 1;
    }

    epoll_event server_event{};
    server_event.data.fd = server_fd;
    server_event.events = EPOLLIN;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &server_event) == -1) {
        perror("epoll_ctl server");
        close(epoll_fd);
        close(server_fd);
        return 1;
    }

    epoll_event events[MAX_EVENTS];
    auto last_sweep = chrono::steady_clock::now();

    while (!shutdown_flag) {
        // 1s timeout so the stale-connection sweep still runs on an idle server.
        int num_ready = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
        if (num_ready == -1) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < num_ready; i++) {
            int ready_fd = events[i].data.fd;
            uint32_t flags = events[i].events;

            if (ready_fd == server_fd) {
                accept_clients(epoll_fd, server_fd);
                continue;
            }

            auto it = clients.find(ready_fd);
            if (it == clients.end()) continue;
            Client& client = it->second;

            if (!handle_io(epoll_fd, client, flags)) client.alive = false;
            if (flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) client.alive = false;

            if (!client.alive) remove_client(epoll_fd, ready_fd);
        }

        for (int fd : dead_fds) remove_client(epoll_fd, fd);   // remove_client ignores unknown fds
        dead_fds.clear();

        auto now = chrono::steady_clock::now();
        if (now - last_sweep >= chrono::seconds(1)) {
            sweep_stale_clients(epoll_fd);
            last_sweep = now;
        }
    }

    log(LogLevel::INFO, "Shutdown signal received. Closing all client connections...");

    for (auto& [fd, client] : clients) {
        if (client.ssl) {
            if (client.tls_state == TlsState::ESTABLISHED) {
                ERR_clear_error();
                SSL_shutdown(client.ssl);
            }
            SSL_free(client.ssl);
        }
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
    }
    clients.clear();

    SSL_CTX_free(g_ssl_ctx);
    close(server_fd);
    close(epoll_fd);
    log(LogLevel::INFO, "Server shut down gracefully.");
    return 0;
}
