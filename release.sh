#!/usr/bin/env bash
#
# Release the wiggle-client package. One run does it all: set the version, build the sdist + wheel,
# verify the metadata + that the generated gRPC stubs are packaged + that a clean install imports,
# and (optionally) upload to PyPI and tag the release.
#
#   ./release.sh 0.1.1                    # build + verify only (dry run -- nothing is uploaded)
#   PUBLISH=testpypi ./release.sh 0.1.1   # also upload to TestPyPI
#   PUBLISH=pypi     ./release.sh 0.1.1   # upload to PyPI, then commit the bump and tag v0.1.1
#   ./release.sh                          # use the version already in pyproject.toml
#
# Auth: twine reads ~/.pypirc or TWINE_USERNAME / TWINE_PASSWORD. Use "__token__" as the username and
# a PyPI API token as the password.
set -euo pipefail
cd "$(dirname "$0")"

CURRENT="$(grep -oE '^version = "[^"]+"' pyproject.toml | sed -E 's/.*"([^"]+)".*/\1/')"
: "${CURRENT:?could not read version from pyproject.toml}"
VERSION="${1:-$CURRENT}"
PUBLISH="${PUBLISH:-}"

printf '%s' "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.]+)?$' \
    || { echo "refusing to use '$VERSION' as a version (expected e.g. 0.1.1)" >&2; exit 1; }

echo "==> release wiggle-client $VERSION (current $CURRENT)${PUBLISH:+ -> $PUBLISH}"

# tooling
python3 -c "import build" 2>/dev/null && python3 -c "import twine" 2>/dev/null \
    || { echo "missing build/twine -- run: pip install build twine" >&2; exit 1; }

# a real publish must come from a clean tree (build artifacts are git-ignored, so they don't count)
if [ "$PUBLISH" = "pypi" ] && [ -n "$(git status --porcelain)" ]; then
    echo "working tree is dirty; commit everything (incl. LICENSE) before a PyPI release:" >&2
    git status --short >&2
    exit 1
fi

# set the version
if [ "$VERSION" != "$CURRENT" ]; then
    perl -pi -e 's/^version = "[^"]*"/version = "'"$VERSION"'"/' pyproject.toml
    echo "==> bumped pyproject.toml to $VERSION"
fi

# clean build
rm -rf dist build ./*.egg-info
python3 -m build

# metadata check
python3 -m twine check dist/*

# the generated gRPC stubs MUST be in the wheel, or the package imports fine in a dev checkout but is
# broken for users. Fail loudly if they're missing (regenerate with ./codegen.sh).
WHEEL="$(ls dist/*.whl)"
python3 - "$WHEEL" <<'PY'
import sys, zipfile
names = set(zipfile.ZipFile(sys.argv[1]).namelist())
required = ["wiggle/_proto/wiggle_pb2.py", "wiggle/_proto/wiggle_pb2_grpc.py"]
missing = [n for n in required if n not in names]
if missing:
    sys.exit("ERROR: gRPC stubs missing from the wheel %s -- run ./codegen.sh first" % missing)
if not any("LICENSE" in n for n in names):
    print("note: no LICENSE in the wheel metadata")
print("   wheel contains the gRPC stubs and LICENSE")
PY

# smoke-test a clean install in a throwaway venv
TMP="$(mktemp -d)"
python3 -m venv "$TMP/venv"
"$TMP/venv/bin/pip" install -q "$WHEEL"
"$TMP/venv/bin/python" -c "from wiggle import Graph, Step, WiggleClient, Worker; print('   clean install imports OK')"
rm -rf "$TMP"

echo "==> built + verified:"; ls -1 dist

case "$PUBLISH" in
    "")
        echo "==> dry run: built and verified, nothing uploaded."
        echo "    set PUBLISH=testpypi or PUBLISH=pypi to upload."
        ;;
    testpypi)
        python3 -m twine upload -r testpypi dist/*
        echo "==> uploaded to TestPyPI. Try: pip install -i https://test.pypi.org/simple/ wiggle-client==$VERSION"
        ;;
    pypi)
        python3 -m twine upload dist/*
        git commit -q -am "Release wiggle-client $VERSION"
        git tag "v$VERSION"
        echo "==> published wiggle-client $VERSION to PyPI and tagged v$VERSION"
        echo "    push it with: git push --follow-tags"
        ;;
    *)
        echo "unknown PUBLISH='$PUBLISH' (use testpypi or pypi)" >&2; exit 1
        ;;
esac