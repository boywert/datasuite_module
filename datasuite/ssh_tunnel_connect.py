# ssh_tunnel_connect — SSH port-forwarding wrapper
#
# Provides two tunnelling backends behind a single interface:
#   1. ``'ssh'``    — spawns a system ``ssh -N -L`` subprocess (used by DB2Connect)
#   2. ``'python'`` — uses ``sshtunnel.SSHTunnelForwarder`` (used by ImpalaConnect)
#
# An available local port in the 8801–8899 range is auto-selected via psutil
# before opening the tunnel, so callers never need to worry about port clashes.
#
# The 'ssh' backend keeps ssh in the foreground (no ``-f``) so the Popen
# handle *is* the tunnel process: ``close()`` really stops it, and a finalizer
# kills it if the object is garbage-collected or the interpreter exits.
#
# Both DB2Connect and ImpalaConnect delegate their tunnel management here,
# and parquet_store._maybe_open_tunnel() uses SSHTunnelConnect as well.

import socket
import subprocess
import tempfile
import time
import weakref

try:
    import psutil
except ImportError:
    psutil = None

try:
    from sshtunnel import SSHTunnelForwarder
except ImportError:
    SSHTunnelForwarder = None


def _kill_process(proc):
    """Terminate *proc* if it is still running (finalizer-safe)."""
    try:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
    except Exception:
        pass


def _port_is_open(port, host="127.0.0.1"):
    """Return True if something accepts TCP connections on host:port."""
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


class SSHTunnelConnect:
    def __init__(self, dest_server=None, dest_port=None, remote_server=None, remote_server_port=None,
                 remote_user=None, remote_private_key=None, tunnel_type="ssh", connect_timeout=30):
        self.dest_server = dest_server
        self.dest_port = dest_port
        self.remote_server = remote_server
        self.remote_server_port = remote_server_port
        self.remote_user = remote_user
        self.remote_private_key = remote_private_key
        self.tunnel_type = tunnel_type
        self.connect_timeout = connect_timeout  # seconds to wait for the 'ssh' tunnel
        self.tunnel = None
        self.local_port = None
        self.pid = None
        self._finalizer = None
        self._errlog = None

    def port_selection(self, port_start=8801, port_end=8899):
        """Return the first local port in the range that is not in use, or False."""
        if psutil is None:
            raise ImportError("psutil is required for SSH tunnel port selection: pip install psutil")
        used = {c.laddr.port for c in psutil.net_connections() if c.laddr}
        for port in range(port_start, port_end + 1):
            if port not in used:
                return port
        return False

    def _ssh_command(self):
        cmd = [
            "ssh",
            "-p", str(self.remote_server_port),
            "-o", "ExitOnForwardFailure=yes",
            "-o", "StrictHostKeyChecking=no",
            "-N",
            "-L", "%d:%s:%d" % (self.local_port, self.dest_server, self.dest_port),
        ]
        if self.remote_private_key:
            cmd += ["-i", str(self.remote_private_key)]
        cmd.append("%s@%s" % (self.remote_user, self.remote_server))
        return cmd

    def _start_ssh(self):
        """Start ``ssh -N -L`` and wait until the local port accepts connections."""
        # stderr goes to a temp file, not a pipe: a long-lived ssh that keeps
        # logging (e.g. failed channel opens) would block on a full pipe.
        errlog = tempfile.TemporaryFile()
        try:
            proc = subprocess.Popen(
                self._ssh_command(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=errlog,
            )
        except BaseException:
            errlog.close()
            raise
        self._errlog = errlog
        self.tunnel = proc
        self.pid = proc.pid
        self._finalizer = weakref.finalize(self, _kill_process, proc)

        deadline = time.monotonic() + self.connect_timeout
        while True:
            if proc.poll() is not None:
                errlog.seek(0)
                err = errlog.read().decode(errors="replace").strip()
                raise ConnectionError(
                    "ssh tunnel exited with code %d%s" % (proc.returncode, (": " + err) if err else "")
                )
            if _port_is_open(self.local_port):
                return
            if time.monotonic() > deadline:
                raise TimeoutError(
                    "ssh tunnel to %s via %s did not open local port %d within %ss"
                    % (self.dest_server, self.remote_server, self.local_port, self.connect_timeout)
                )
            time.sleep(0.2)

    def connect(self):
        """Open the tunnel. Raises on failure (after cleaning up)."""
        if self.tunnel:
            self.close()
        try:
            self.local_port = self.port_selection()
            if not self.local_port:
                raise RuntimeError("No free local port in range 8801-8899 for the SSH tunnel")
            if self.tunnel_type == 'ssh':
                self._start_ssh()
                print("Using port %d to connect to %s:%d via %s in PID %d." % (
                    self.local_port, self.dest_server, self.dest_port, self.remote_server, self.pid))
            elif self.tunnel_type == 'python':
                if SSHTunnelForwarder is None:
                    raise ImportError("sshtunnel is required for tunnel_type='python': pip install sshtunnel")
                self.tunnel = SSHTunnelForwarder(
                    (self.remote_server, self.remote_server_port),
                    ssh_username=self.remote_user,
                    ssh_private_key=self.remote_private_key,
                    remote_bind_address=(self.dest_server, self.dest_port),
                    local_bind_address=('127.0.0.1', self.local_port),
                )
                self.tunnel.start()
                print("Using port %d to connect to %s:%d via %s using SSHTunnelForwarder" % (
                    self.local_port, self.dest_server, self.dest_port, self.remote_server))
            else:
                raise ValueError("tunnel_type must be 'ssh' or 'python'; got %r" % (self.tunnel_type,))
        except BaseException:
            self._teardown()
            raise

    def _teardown(self):
        """Stop the tunnel if one exists; never raises."""
        tunnel, self.tunnel = self.tunnel, None
        if tunnel is None:
            return
        try:
            if self.tunnel_type == 'ssh':
                _kill_process(tunnel)
                if self._finalizer is not None:
                    self._finalizer.detach()
                    self._finalizer = None
                if getattr(self, "_errlog", None) is not None:
                    self._errlog.close()
                    self._errlog = None
            elif self.tunnel_type == 'python':
                tunnel.stop()
        except Exception:
            pass

    def close(self):
        if self.tunnel:
            self._teardown()
            if self.tunnel_type == 'ssh':
                print("PID: %d port %d. Connection closed." % (self.pid, self.local_port))
            else:
                print("SSHtunnelForwarder port %d. Connection closed." % (self.local_port))
        else:
            print("No active connection to close.")
