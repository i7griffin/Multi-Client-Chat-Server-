#!/usr/bin/env bash
# Generates a private CA + a server certificate for local development.
# ECDSA P-256 keys: much faster handshakes than RSA-2048/4096 at equal security.
# Usage: ./gen_certs.sh [output_dir]     (default: certs)
set -euo pipefail

DIR="${1:-certs}"
mkdir -p "$DIR"
cd "$DIR"

# 1) Certificate Authority (the client trusts ONLY this)
openssl ecparam -name prime256v1 -genkey -noout -out ca.key
openssl req -x509 -new -key ca.key -sha256 -days 3650 \
    -subj "/CN=ChatServer Dev CA" -out ca.crt

# 2) Server key + certificate signed by that CA.
#    The SAN list MUST contain every name/IP clients connect to.
openssl ecparam -name prime256v1 -genkey -noout -out server.key
openssl req -new -key server.key -subj "/CN=localhost" -out server.csr
cat > server.ext <<EXT
subjectAltName=DNS:localhost,IP:127.0.0.1
basicConstraints=CA:FALSE
keyUsage=digitalSignature
extendedKeyUsage=serverAuth
EXT
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days 825 -sha256 -extfile server.ext

chmod 600 ca.key server.key
rm -f server.csr server.ext ca.srl
echo "Done. Server uses: $DIR/server.crt + $DIR/server.key   Client trusts: $DIR/ca.crt"
echo "NEVER commit *.key files to GitHub (add certs/*.key to .gitignore)."
