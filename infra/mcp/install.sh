#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root"
  exit 1
fi

src_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
install_dir="/opt/mpunit-mcp"

apt-get update
apt-get install -y python3-venv python3-pip acl nginx

if ! id mpunit-mcp >/dev/null 2>&1; then
  useradd --system --home-dir "${install_dir}" --shell /usr/sbin/nologin mpunit-mcp
fi

mkdir -p "${install_dir}"
install -m 0644 "${src_dir}/server.py" "${install_dir}/server.py"
install -m 0644 "${src_dir}/requirements.txt" "${install_dir}/requirements.txt"
python3 -m venv "${install_dir}/.venv"
"${install_dir}/.venv/bin/pip" install --upgrade pip
"${install_dir}/.venv/bin/pip" install -r "${install_dir}/requirements.txt"

for path in /srv/mp-unit-staging /opt/ozon-unit; do
  if [[ -d "${path}" ]]; then
    setfacl -Rm u:mpunit-mcp:rwX "${path}"
    setfacl -Rdm u:mpunit-mcp:rwX "${path}"
  fi
done

usermod -a -G systemd-journal mpunit-mcp || true

install -m 0644 "${src_dir}/mpunit-mcp.service" /etc/systemd/system/mpunit-mcp.service

cat >/etc/sudoers.d/mpunit-mcp <<'EOF'
mpunit-mcp ALL=(root) NOPASSWD: /usr/bin/systemctl restart ozon-unit, /usr/bin/systemctl restart ozon-unit-auto-sync
EOF
chmod 0440 /etc/sudoers.d/mpunit-mcp
visudo -cf /etc/sudoers.d/mpunit-mcp

if [[ ! -f /etc/mpunit-mcp.env ]]; then
  cat >/etc/mpunit-mcp.env <<'EOF'
MCP_PUBLIC_URL=https://mcp.mp-unit.ru/mcp
AUTH0_ISSUER=https://CHANGE-ME.eu.auth0.com/
AUTH0_AUDIENCE=https://mcp.mp-unit.ru/mcp
MCP_READ_SCOPE=mpunit:read
MCP_WRITE_SCOPE=mpunit:write
MCP_PORT=8765
EOF
  chmod 0600 /etc/mpunit-mcp.env
fi

install -m 0644 "${src_dir}/nginx-mcp.conf" /etc/nginx/sites-available/mpunit-mcp
ln -sfn /etc/nginx/sites-available/mpunit-mcp /etc/nginx/sites-enabled/mpunit-mcp
nginx -t
systemctl reload nginx
systemctl daemon-reload

echo
echo "installed. now:"
echo "1) edit /etc/mpunit-mcp.env and set AUTH0_ISSUER"
echo "2) point mcp.mp-unit.ru DNS to this server"
echo "3) obtain TLS certificate for mcp.mp-unit.ru"
echo "4) systemctl enable --now mpunit-mcp"
echo "5) test: systemctl status mpunit-mcp --no-pager"
