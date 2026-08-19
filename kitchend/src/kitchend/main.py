"""kitchend CLI: serve | status | index."""

import argparse
import json
import sys
import urllib.error
import urllib.request

from kitchend.config import CONFIG_PATH, load_config


def main(argv=None):
    parser = argparse.ArgumentParser(prog="kitchend", description=__doc__)
    parser.add_argument("--config", default=None, help=f"config path (default {CONFIG_PATH})")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve", help="run the daemon")
    sub.add_parser("status", help="ping a running daemon")
    sub.add_parser("index", help="(re)index run directories into the results catalog")
    args = parser.parse_args(argv)

    from pathlib import Path
    config = load_config(Path(args.config) if args.config else None)

    if args.cmd == "serve":
        import uvicorn

        from kitchend.api.app import create_app
        uvicorn.run(create_app(config), host=config.bind_host, port=config.bind_port)
    elif args.cmd == "status":
        url = f"http://{config.bind_host}:{config.bind_port}/api/health"
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                print(json.dumps(json.load(resp), indent=2))
        except (urllib.error.URLError, OSError) as e:
            print(f"daemon not reachable at {url}: {e}", file=sys.stderr)
            return 1
    elif args.cmd == "index":
        print("results-catalog indexing lands in a later milestone", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
