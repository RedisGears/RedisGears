#!/bin/bash

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

$HERE/generate_tests_cert.sh

$HERE/run_tests.sh --tls --tls-cert-file ./tests/tls/redis.crt --tls-key-file ./tests/tls/redis.key --tls-ca-cert-file ./tests/tls/ca.crt --tls-passphrase foobar "$@"
