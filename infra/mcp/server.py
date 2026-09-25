import hashlib
import os
import shlex
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Literal

import jwt
from jwt import PyJWKClient
from pydantic import AnyHttpUrl
from mcp.server import MCPServer
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings

PUBLIC_URL = os.environ["MCP_PUBLIC_URL"].rstrip("/")
ISSUER = os.environ["AUTH0_ISSUER"].rstrip("/") + "/"
AUDIENCE = os.getenv("AUTH0_AUDIENCE", PUBLIC_URL)
READ_SCOPE = os.getenv("MCP_READ_SCOPE", "mpunit:read")
WRITE_SCOPE = os.getenv("MCP_WRITE_SCOPE", "mpunit:write")
PORT = int(os.getenv("MCP_PORT", "8765"))

ROOTS = {
    "staging": Path("/srv/mp-unit-staging"),
    "production": Path("/opt/ozon-unit"),
}
SERVICES = {"ozon-unit", "ozon-unit-auto-sync"}
JWK = PyJWKClient(ISSUER + ".well-known/jwks.json")

class Auth0Verifier(TokenVerifier):
    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            key = JWK.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                key.key,
                algorithms=["RS256"],
                audience=AUDIENCE,
                issuer=ISSUER,
            )
            scopes = str(claims.get("scope", "")).split()
            return AccessToken(
                token=token,
                client_id=str(claims.get("azp") or claims.get("client_id") or "chatgpt"),
                scopes=scopes,
                expires_at=int(claims["exp"]) if claims.get("exp") else None,
                resource=PUBLIC_URL,
                subject=claims.get("sub"),
                claims=claims,
            )
        except Exception:
            return None

mcp = MCPServer(
    "MP Unit Server Operations",
    token_verifier=Auth0Verifier(),
    auth=AuthSettings(
        issuer_url=AnyHttpUrl(ISSUER),
        resource_server_url=AnyHttpUrl(PUBLIC_URL),
        required_scopes=[READ_SCOPE, WRITE_SCOPE],
        validate_token_resource=True,
    ),
)

def require_write() -> None:
    token = get_access_token()
    if token is None or WRITE_SCOPE not in token.scopes:
        raise PermissionError(f"OAuth scope {WRITE_SCOPE!r} is required")

def root_for(target: Literal["staging", "production"]) -> Path:
    root = ROOTS[target].resolve()
    if not root.exists():
        raise FileNotFoundError(str(root))
    return root

def safe_path(target: Literal["staging", "production"], relative_path: str) -> Path:
    if not relative_path or Path(relative_path).is_absolute():
        raise ValueError("relative_path must be relative")
    root = root_for(target)
    path = (root / relative_path).resolve()
    if path != root and root not in path.parents:
        raise ValueError("path escapes allowed root")
    return path

def run(argv: list[str], cwd: Path | None = None, timeout: int = 60) -> dict:
    started = time.time()
    proc = subprocess.run(
        argv,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
        timeout=max(1, min(timeout, 300)),
        env={**os.environ, "LC_ALL": "C.UTF-8", "LANG": "C.UTF-8"},
    )
    return {
        "argv": argv,
        "cwd": str(cwd) if cwd else None,
        "returncode": proc.returncode,
        "stdout": proc.stdout[-50000:],
        "stderr": proc.stderr[-50000:],
        "seconds": round(time.time() - started, 3),
    }

@mcp.tool()
def server_health() -> dict:
    """Return basic host and service health without changing anything."""
    result = {"hostname": socket.gethostname(), "roots": {}}
    for name, root in ROOTS.items():
        result["roots"][name] = {"path": str(root), "exists": root.exists()}
    result["ozon_unit"] = run(["systemctl", "is-active", "ozon-unit"])
    return result

@mcp.tool()
def list_directory(
    target: Literal["staging", "production"],
    relative_path: str = ".",
    max_items: int = 300,
) -> list[dict]:
    """List files under an allowed mp-unit root."""
    path = safe_path(target, relative_path)
    if not path.is_dir():
        raise NotADirectoryError(str(path))
    items = []
    for child in sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))[:max_items]:
        stat = child.stat()
        items.append({
            "name": child.name,
            "type": "dir" if child.is_dir() else "file",
            "size": stat.st_size,
            "mtime": int(stat.st_mtime),
        })
    return items

@mcp.tool()
def read_file(
    target: Literal["staging", "production"],
    relative_path: str,
    start_line: int = 1,
    end_line: int = 400,
) -> dict:
    """Read a bounded line range from a text file under an allowed root."""
    path = safe_path(target, relative_path)
    if path.stat().st_size > 2_000_000:
        raise ValueError("file is larger than 2 MB")
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    start = max(1, start_line)
    end = max(start, min(end_line, start + 999))
    selected = lines[start - 1:end]
    return {
        "path": str(path),
        "start_line": start,
        "end_line": min(end, len(lines)),
        "total_lines": len(lines),
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "content": "\n".join(selected),
    }

@mcp.tool()
def git_status(target: Literal["staging", "production"]) -> dict:
    """Return git status for an allowed mp-unit root."""
    return run(["git", "status", "--short", "--branch"], root_for(target))

@mcp.tool()
def git_diff(
    target: Literal["staging", "production"],
    relative_path: str | None = None,
) -> dict:
    """Return the current git diff, optionally for one relative path."""
    argv = ["git", "diff", "--"]
    if relative_path:
        safe_path(target, relative_path)
        argv.append(relative_path)
    return run(argv, root_for(target))

@mcp.tool()
def git_log(target: Literal["staging", "production"], count: int = 20) -> dict:
    """Return recent commits for an allowed mp-unit root."""
    count = max(1, min(count, 100))
    return run(
        ["git", "log", f"-n{count}", "--date=iso", "--pretty=format:%h %ad %an %s"],
        root_for(target),
    )

@mcp.tool()
def write_file(
    target: Literal["staging", "production"],
    relative_path: str,
    content: str,
    expected_sha256: str | None = None,
) -> dict:
    """Create or replace one text file. Requires mpunit:write and supports optimistic locking."""
    require_write()
    path = safe_path(target, relative_path)
    if path.exists() and expected_sha256:
        current = path.read_bytes()
        actual = hashlib.sha256(current).hexdigest()
        if actual != expected_sha256:
            raise RuntimeError(f"sha256 mismatch: expected {expected_sha256}, found {actual}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".mcp-tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)
    digest = hashlib.sha256(content.encode()).hexdigest()
    return {"path": str(path), "bytes": len(content.encode()), "sha256": digest}

@mcp.tool()
def replace_text(
    target: Literal["staging", "production"],
    relative_path: str,
    old_text: str,
    new_text: str,
    expected_sha256: str | None = None,
) -> dict:
    """Replace one exact text occurrence in a file. Requires mpunit:write."""
    require_write()
    path = safe_path(target, relative_path)
    original = path.read_text(encoding="utf-8")
    actual = hashlib.sha256(original.encode()).hexdigest()
    if expected_sha256 and actual != expected_sha256:
        raise RuntimeError(f"sha256 mismatch: expected {expected_sha256}, found {actual}")
    count = original.count(old_text)
    if count != 1:
        raise RuntimeError(f"old_text must occur exactly once; found {count}")
    updated = original.replace(old_text, new_text, 1)
    tmp = path.with_name(path.name + ".mcp-tmp")
    tmp.write_text(updated, encoding="utf-8")
    os.replace(tmp, path)
    return {
        "path": str(path),
        "old_sha256": actual,
        "new_sha256": hashlib.sha256(updated.encode()).hexdigest(),
    }

ALLOWED_EXECUTABLES = {
    "git", "python", "python3", "pytest", "ruff", "mypy",
    "node", "npm", "grep", "rg", "find", "ls", "head", "tail", "wc", "pwd",
}
ALLOWED_GIT_SUBCOMMANDS = {"status", "diff", "log", "show", "rev-parse", "ls-files"}

@mcp.tool()
def run_command(
    target: Literal["staging", "production"],
    command: str,
    timeout: int = 60,
) -> dict:
    """Run a bounded developer command without a shell. Requires mpunit:write."""
    require_write()
    argv = shlex.split(command)
    if not argv:
        raise ValueError("empty command")
    executable = Path(argv[0]).name
    if executable not in ALLOWED_EXECUTABLES:
        raise PermissionError(f"executable {executable!r} is not allowed")
    if executable == "git" and (len(argv) < 2 or argv[1] not in ALLOWED_GIT_SUBCOMMANDS):
        raise PermissionError("only read-only git subcommands are allowed")
    return run(argv, root_for(target), timeout)

@mcp.tool()
def service_status(service: Literal["ozon-unit", "ozon-unit-auto-sync"]) -> dict:
    """Show systemd status for an approved mp-unit service."""
    return run(["systemctl", "status", service, "--no-pager", "--full"])

@mcp.tool()
def journal_tail(
    service: Literal["ozon-unit", "ozon-unit-auto-sync"],
    lines: int = 200,
) -> dict:
    """Read recent journal lines for an approved mp-unit service."""
    lines = max(1, min(lines, 2000))
    return run(["journalctl", "-u", service, "-n", str(lines), "--no-pager", "-o", "short-iso"])

@mcp.tool()
def restart_service(service: Literal["ozon-unit", "ozon-unit-auto-sync"]) -> dict:
    """Restart an approved mp-unit service. Requires mpunit:write."""
    require_write()
    if service not in SERVICES:
        raise PermissionError("service not allowed")
    return run(["sudo", "-n", "systemctl", "restart", service], timeout=30)

@mcp.tool()
def deploy_paths(paths: list[str]) -> dict:
    """Copy selected relative paths from staging to production with timestamped backups. Requires mpunit:write."""
    require_write()
    if not paths or len(paths) > 100:
        raise ValueError("provide 1..100 paths")
    staging = root_for("staging")
    production = root_for("production")
    stamp = time.strftime("%Y%m%d-%H%M%S")
    backup_root = production / ".mcp-backups" / stamp
    changed = []
    for rel in paths:
        src = safe_path("staging", rel)
        dst = safe_path("production", rel)
        if not src.is_file():
            raise FileNotFoundError(str(src))
        if dst.exists():
            backup = backup_root / rel
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(dst, backup)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        changed.append(rel)
    return {"deployed": changed, "backup_root": str(backup_root)}

if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="127.0.0.1",
        port=PORT,
        json_response=True,
        stateless_http=True,
    )
