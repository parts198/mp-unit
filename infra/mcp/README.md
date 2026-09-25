# MP Unit remote MCP gateway

This is a standalone MCP operations gateway for the existing mp-unit servers. It does not replace the application or require the current production tree to be pulled from GitHub.

## Security model

- Runs as a dedicated Unix account: `mpunit-mcp`.
- File access is limited by ACL to `/srv/mp-unit-staging` and `/opt/ozon-unit`.
- No arbitrary shell is exposed. `run_command` uses argv execution without a shell and an executable allowlist.
- `git` is limited to read-only subcommands.
- systemd restart is sudo-allowlisted only for `ozon-unit` and `ozon-unit-auto-sync`.
- OAuth read scope: `mpunit:read`.
- OAuth write scope: `mpunit:write`.
- Production writes and service restarts require `mpunit:write`.

## Auth0

Create an Auth0 API whose Identifier is exactly:

`https://mcp.mp-unit.ru/mcp`

Add API permissions:

- `mpunit:read`
- `mpunit:write`

Enable Auth for MCP / third-party client registration (CIMD or DCR) in the tenant. Set the tenant default audience to the same API Identifier. Auth0 should issue RS256 access tokens for that audience.

Set `AUTH0_ISSUER` in `/etc/mpunit-mcp.env` to the tenant issuer, for example:

`https://example.eu.auth0.com/`

## Install on Ubuntu

From this directory:

```bash
sudo bash install.sh
```

Then edit:

```bash
sudo nano /etc/mpunit-mcp.env
```

Start the local MCP service:

```bash
sudo systemctl enable --now mpunit-mcp
sudo systemctl status mpunit-mcp --no-pager
sudo journalctl -u mpunit-mcp -n 100 --no-pager
```

## DNS and TLS

Create an A/AAAA record for `mcp.mp-unit.ru` pointing to the server. The installer creates the nginx HTTP vhost.

After DNS resolves, obtain a certificate. If certbot is already the host's certificate manager:

```bash
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d mcp.mp-unit.ru
```

Then verify:

```bash
curl -i https://mcp.mp-unit.ru/mcp
```

An unauthenticated MCP request should be rejected with OAuth discovery information; that is expected.

## Connect ChatGPT

Enable Developer mode in ChatGPT, create a personal MCP/plugin connection, choose OAuth authentication, and use:

`https://mcp.mp-unit.ru/mcp`

After OAuth login, verify read tools first: `server_health`, `git_status`, `service_status`, `journal_tail`. Only then test a write operation in staging.

## Initial tool surface

Read:
- `server_health`
- `list_directory`
- `read_file`
- `git_status`
- `git_diff`
- `git_log`
- `service_status`
- `journal_tail`

Write:
- `write_file`
- `replace_text`
- `run_command`
- `restart_service`
- `deploy_paths`
