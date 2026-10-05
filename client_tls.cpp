// client_tls.cpp
// TLS 1.3 client for the epoll chat server. Verifies the server certificate
// against your CA and checks that it was issued for the host you typed.
//
// Build:  g++ -std=c++17 -O2 -Wall -Wextra client_tls.cpp -o client_tls -lssl -lcrypto
// Run:    ./client_tls [host] [port] [ca.crt]     (defaults: 127.0.0.1 8080 certs/ca.crt)
//
// Why no threads any more: one SSL* object must not be used by two threads at
// once, and the old design had one thread blocked in recv() while another sent.
// A single poll() loop over {stdin, socket} is thread-safe by construction,
// needs no mutex, and is cheaper.

#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <cstdint>
#include <cerrno>
#include <csignal>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>

using namespace std;

const size_t MAX_MESSAGE_SIZE = 1024;

// Must match the server's enum exactly.
enum class MessageType : uint8_t { USERNAME = 0, CHAT = 1 };

void print_ssl_errors(const string& context) {
    unsigned long e;
    while ((e = ERR_get_error()) != 0) {
        char buf[256];
        ERR_error_string_n(e, buf, sizeof(buf));
        cerr << context << ": " << buf << endl;
    }
}

// Writes the whole buffer through TLS on a NON-BLOCKING socket, waiting with
// poll() whenever OpenSSL says it needs the socket to become ready.
bool ssl_write_all(SSL* ssl, int fd, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ERR_clear_error();
        int r = SSL_write(ssl, data + sent, static_cast<int>(len - sent));
        if (r > 0) {
            sent += static_cast<size_t>(r);
            continue;
        }
        int err = SSL_get_error(ssl, r);
        if (err == SSL_ERROR_WANT_WRITE || err == SSL_ERROR_WANT_READ) {
            pollfd p{fd, static_cast<short>(err == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT), 0};
            if (poll(&p, 1, -1) == -1 && errno != EINTR) return false;
            continue;
        }
        print_ssl_errors("SSL_write");
        return false;
    }
    return true;
}

// Builds [type][len][payload] and sends it as ONE write -> one TLS record
// (the old client made three separate send() calls per message).
bool send_message(SSL* ssl, int fd, MessageType type, const string& message) {
    if (message.size() > MAX_MESSAGE_SIZE) return false;

    uint8_t type_byte = static_cast<uint8_t>(type);
    uint32_t network_length = htonl(static_cast<uint32_t>(message.size()));

    string packet;
    packet.reserve(1 + 4 + message.size());
    packet.append(reinterpret_cast<const char*>(&type_byte), 1);
    packet.append(reinterpret_cast<const char*>(&network_length), 4);
    packet.append(message);

    return ssl_write_all(ssl, fd, packet.data(), packet.size());
}

// Reads everything available, prints every complete message.
// Returns false when the server closed the connection or on error.
bool handle_incoming(SSL* ssl, vector<char>& inbuf) {
    char buffer[16 * 1024];
    constexpr size_t HEADER_SIZE = 5;

    while (true) {
        ERR_clear_error();
        int r = SSL_read(ssl, buffer, sizeof(buffer));
        if (r > 0) {
            inbuf.insert(inbuf.end(), buffer, buffer + r);

            size_t pos = 0;
            while (inbuf.size() - pos >= HEADER_SIZE) {
                uint32_t network_length;
                memcpy(&network_length, inbuf.data() + pos + 1, 4);
                uint32_t len = ntohl(network_length);
                if (len > MAX_MESSAGE_SIZE) {
                    cerr << "Server sent an oversized message; disconnecting." << endl;
                    return false;
                }
                if (inbuf.size() - pos < HEADER_SIZE + len) break;

                cout << "Server replied: " << string(inbuf.data() + pos + HEADER_SIZE, len) << endl;
                pos += HEADER_SIZE + len;
            }
            if (pos > 0) inbuf.erase(inbuf.begin(), inbuf.begin() + pos);
            continue;
        }

        int err = SSL_get_error(ssl, r);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) return true;
        if (err == SSL_ERROR_ZERO_RETURN) return false;   // clean close_notify
        print_ssl_errors("SSL_read");
        return false;
    }
}

int main(int argc, char** argv) {
    const string host    = argc > 1 ? argv[1] : "127.0.0.1";
    const string port    = argc > 2 ? argv[2] : "8080";
    const char* ca_file  = argc > 3 ? argv[3] : "certs/ca.crt";

    signal(SIGPIPE, SIG_IGN);

    // ---- TLS context: trust ONLY our CA ----
    SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) { print_ssl_errors("SSL_CTX_new"); return 1; }
    SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
    SSL_CTX_set_options(ctx, SSL_OP_NO_COMPRESSION | SSL_OP_IGNORE_UNEXPECTED_EOF);
    SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    if (SSL_CTX_load_verify_locations(ctx, ca_file, nullptr) != 1) {
        print_ssl_errors(string("Loading CA file ") + ca_file);
        return 1;
    }

    // ---- TCP connect ----
    addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    int gai = getaddrinfo(host.c_str(), port.c_str(), &hints, &res);
    if (gai != 0) {
        cerr << "getaddrinfo: " << gai_strerror(gai) << endl;
        return 1;
    }
    int fd = socket(res->ai_family, res->ai_socktype | SOCK_CLOEXEC, res->ai_protocol);
    if (fd == -1 || connect(fd, res->ai_addr, res->ai_addrlen) == -1) {
        cerr << "Connection failed: " << strerror(errno) << endl;
        freeaddrinfo(res);
        return 1;
    }
    freeaddrinfo(res);
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    // ---- TLS handshake (blocking, before we switch the socket to non-blocking) ----
    SSL* ssl = SSL_new(ctx);
    SSL_set_fd(ssl, fd);

    // Certificate must be valid for the name/IP we connected to. Without this
    // check, anyone with ANY cert from your CA could impersonate the server.
    unsigned char probe[sizeof(in6_addr)];
    bool host_is_ip = inet_pton(AF_INET, host.c_str(), probe) == 1 || inet_pton(AF_INET6, host.c_str(), probe) == 1;
    if (host_is_ip) {
        X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), host.c_str());
    } else {
        SSL_set1_host(ssl, host.c_str());
        SSL_set_tlsext_host_name(ssl, host.c_str());   // SNI
    }

    if (SSL_connect(ssl) != 1) {
        print_ssl_errors("TLS handshake failed");
        long vr = SSL_get_verify_result(ssl);
        if (vr != X509_V_OK) cerr << "Certificate verification: " << X509_verify_cert_error_string(vr) << endl;
        SSL_free(ssl);
        close(fd);
        SSL_CTX_free(ctx);
        return 1;
    }
    cout << "Connected securely: " << SSL_get_version(ssl) << ", " << SSL_get_cipher_name(ssl) << endl;

    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

    // ---- username handshake ----
    string username;
    cout << "Enter username: ";
    if (!getline(cin, username)) {
        cout << "No username entered; exiting." << endl;
        SSL_free(ssl); close(fd); SSL_CTX_free(ctx);
        return 1;
    }
    if (!send_message(ssl, fd, MessageType::USERNAME, username)) {
        cout << "Failed to send username." << endl;
        SSL_free(ssl); close(fd); SSL_CTX_free(ctx);
        return 1;
    }

    // ---- single-threaded event loop ----
    vector<char> inbuf;
    pollfd fds[2];
    fds[0] = {STDIN_FILENO, POLLIN, 0};
    fds[1] = {fd, POLLIN, 0};
    int exit_code = 0;

    while (true) {
        // OpenSSL may hold already-decrypted bytes the socket knows nothing about;
        // handle_incoming() drains until WANT_READ, so that cannot leave data behind.
        if (poll(fds, 2, -1) == -1) {
            if (errno == EINTR) continue;
            perror("poll");
            exit_code = 1;
            break;
        }

        // Handle the socket first so server messages show up promptly.
        if (fds[1].revents & (POLLIN | POLLHUP | POLLERR)) {
            if (!handle_incoming(ssl, inbuf)) {
                cout << "Receive failed or server disconnected." << endl;
                break;
            }
        }

        if ((fds[0].revents & (POLLIN | POLLHUP))) {
            string line;
            if (!getline(cin, line) || line == "quit") break;

            if (!send_message(ssl, fd, MessageType::CHAT, line)) {
                cout << "Send failed." << endl;
                exit_code = 1;
                break;
            }
            cout << "Message sent successfully." << endl;
        }
    }

    // Politely tell the server we are leaving (close_notify), then clean up.
    ERR_clear_error();
    SSL_shutdown(ssl);
    SSL_free(ssl);
    close(fd);
    SSL_CTX_free(ctx);
    return exit_code;
}
