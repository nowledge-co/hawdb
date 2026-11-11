# Prebuilt C ABI libraries

Each published HawDB GitHub Release carries the C ABI (`bindings/ffi`) built
for a fixed platform matrix, so a host can embed a released engine without
compiling Rust. The same libraries are published to npm for the
[Bun binding](../bindings/bun/README.md). Language adapters that do not use the
C ABI still need their own build, and the `hawdb` CLI binary is not an embedding
artifact.

The [Native Release workflow](../.github/workflows/native-release.yml) builds
them when a release is published (or replays an existing tag by
`workflow_dispatch`). Pull requests that change the pipeline build and test
every archive without uploading anything.

## Platform matrix

| Target | Archive | npm package | Built on | Minimum runtime |
| --- | --- | --- | --- | --- |
| macOS arm64 | `hawdb-ffi-<version>-aarch64-apple-darwin.tar.gz` | `@nowledge/hawdb-ffi-darwin-arm64` | `macos-15` | macOS 11.0 |
| macOS x86-64 | `hawdb-ffi-<version>-x86_64-apple-darwin.tar.gz` | `@nowledge/hawdb-ffi-darwin-x64` | `macos-15-intel` | macOS 11.0 |
| Linux x86-64, glibc | `hawdb-ffi-<version>-x86_64-unknown-linux-gnu.tar.gz` | `@nowledge/hawdb-ffi-linux-x64-gnu` | `ubuntu-24.04` | glibc 2.34 |
| Linux AArch64, glibc | `hawdb-ffi-<version>-aarch64-unknown-linux-gnu.tar.gz` | `@nowledge/hawdb-ffi-linux-arm64-gnu` | `ubuntu-24.04-arm` | glibc 2.34 |
| Windows x86-64, MSVC | `hawdb-ffi-<version>-x86_64-pc-windows-msvc.zip` | `@nowledge/hawdb-ffi-win32-x64-msvc` | `windows-2025` | Windows 10; Visual C++ runtime |

The minimum runtime is measured from each packaged binary, not assumed from
the build image: the macOS deployment target is read from `LC_BUILD_VERSION`
and the glibc floor is the highest `GLIBC_*` symbol version the library
imports. Each archive's `manifest.json` records the exact value; the table
shows what the current build images produce (Ubuntu 24.04 ships glibc 2.39,
but the library only imports symbols up to `GLIBC_2.34`). The macOS
floor is set by `--config=prebuilt` in [`.bazelrc`](../.bazelrc). The Windows
DLL uses the dynamic MSVC runtime (`vcruntime140.dll` and the Universal CRT),
which a host installer must provide.

musl Linux, 32-bit, Windows arm64 and other targets have no prebuilt archive;
[build from source](#building-from-source).

## Archive contents

```text
hawdb-ffi-<version>-<target>/
  include/hawdb_ffi.h
  lib/libhawdb_ffi.so | lib/libhawdb_ffi.dylib    # shared library (macOS, Linux)
  lib/libhawdb_ffi.a                              # static archive (macOS, Linux)
  bin/hawdb_ffi.dll, lib/hawdb_ffi.dll.lib        # DLL and import library (Windows)
  LICENSE
  manifest.json
```

`manifest.json` records:

- `version` and `engine_revision`: the release and the exact source commit.
- `feature_profile`: the engine's default Cargo features
  (`background-maintenance`, `full-text-search`, `graph-analytics`,
  `tokio-runtime`, `vector-search`). Other feature selections require a source
  build.
- `build`: Bazel targets `//bindings/ffi:hawdb_ffi` and
  `//bindings/ffi:hawdb_ffi_static` with `--config=prebuilt` (the repository
  release profile plus the platform deployment floor), and the runner image.
- `abi`: the header, the `hawdb_version()` string and the compatibility rule
  below.
- `requirements`: OS, minimum OS or glibc, macOS install name and signature,
  and `static_link_flags`: the system libraries a static link needs.
- `files`: the SHA-256 and size of every other file in the archive.

Windows ships the DLL and its import library only; its static archive is not
link-qualified.

### ABI compatibility

HawDB is pre-1.0. The ordinary C ABI is versioned with the release and may
change between minor versions. Compile against the header from the same
archive, load only that library, and treat a version change as a rebuild of
the host. `hawdb_version()` returns `hawdb <version>`; check it at startup if
the library is located at run time. The `hawdb_retained_*` exports are
experimental (retained descriptor ABI version 1).

## Download and verify

Every release also carries `SHA256SUMS` covering all five archives:

```sh
version=0.31.0
target=x86_64-unknown-linux-gnu
gh release download "v$version" -R nowledge-co/hawdb \
  -p "hawdb-ffi-$version-$target.tar.gz" -p SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS      # macOS: shasum -a 256 -c --ignore-missing SHA256SUMS
tar -xzf "hawdb-ffi-$version-$target.tar.gz"
```

Without `gh`, download the same files from
`https://github.com/nowledge-co/hawdb/releases/download/v<version>/<file>`.
The per-file hashes in `manifest.json` cover the extracted contents.

## Link or load

[`bindings/ffi/examples/embed.c`](../bindings/ffi/examples/embed.c) opens an
in-memory database and runs one query. The release workflow compiles and runs
it against every archive, both ways on macOS and Linux:

```sh
root="hawdb-ffi-$version-$target"

# Shared library, found through the rpath at run time.
cc -I "$root/include" bindings/ffi/examples/embed.c \
  -L "$root/lib" -lhawdb_ffi -Wl,-rpath,"$PWD/$root/lib" -o embed
./embed

# Static archive with the system libraries the manifest lists.
cc -I "$root/include" bindings/ffi/examples/embed.c "$root/lib/libhawdb_ffi.a" \
  $(python3 -c 'import json,sys; print(" ".join(json.load(open(sys.argv[1]))["requirements"]["static_link_flags"]))' "$root/manifest.json") \
  -o embed-static
```

On Windows, from a Visual Studio developer prompt:

```bat
cl /I %ROOT%\include bindings\ffi\examples\embed.c %ROOT%\lib\hawdb_ffi.dll.lib
set PATH=%ROOT%\bin;%PATH%
embed.exe
```

Hosts that map the library at run time pass its path explicitly, for example
`hawdb.LoadLibrary(path)` in [Go](../bindings/go/README.md) or
`loadHawDB(path)` in [Bun](../bindings/bun/README.md).

### macOS install name and signing

The dylib's install name is `@rpath/libhawdb_ffi.dylib`. Put it in your
bundle (for example `Contents/Frameworks/`) and add a matching `LC_RPATH` to
the host (`-Wl,-rpath,@executable_path/../Frameworks`).

The archive's dylib carries only an ad-hoc signature, which macOS requires to
load arm64 code after the install name is rewritten. Signing with a Developer
ID, hardened runtime and notarization are the embedding application's
responsibility: sign the dylib with your own identity when you sign the app.

## Building from source

Unsupported targets and custom feature profiles build the same targets from a
checkout of the release tag:

```sh
bazel build --config=prebuilt //bindings/ffi:hawdb_ffi //bindings/ffi:hawdb_ffi_static
```

or, with Cargo and the binding's own lockfile, `cargo build --release` in
`bindings/ffi` (see [the C ABI notes](../bindings/ffi/README.md)). Package a
Bazel build the same way as a release with
[`scripts/package-ffi-release.py`](../scripts/package-ffi-release.py).

## npm packages

`@nowledge/hawdb-bun` is the Bun binding. It lists one
`@nowledge/hawdb-ffi-<platform>` package per row above as an optional
dependency, pinned to the same version; each platform package declares `os`,
`cpu` and, on Linux, `libc`, so a package manager installs only the matching
one. It contains the shared library, `hawdb_ffi.h`, LICENSE and a README.
`loadHawDB()` without a path resolves that installed package; an explicit
path still loads exactly that file. No environment variable is consulted.
On Linux it chooses glibc or musl from the program interpreter of the running
Bun executable (`/proc/self/exe`), not from loader files on disk, so a glibc
host that also has musl installed still selects the glibc package. The exact
`optionalDependencies` pin does not constrain module resolution, so
`loadHawDB()` also reads the resolved platform package's `package.json` and
refuses one whose version differs from `@nowledge/hawdb-bun`'s (for example an
older copy left after installing with optional dependencies disabled).

Package versions equal the HawDB release version. The release workflow
assembles the packages from the archives it just uploaded (so npm and the
GitHub Release ship the same bytes) and publishes the platform packages before
the main package. A version already on npm is skipped when its tarball shasum
matches and refused otherwise, so retries are safe.

### npm trusted publishing

The `npm` job publishes with
[npm trusted publishing](https://docs.npmjs.com/trusted-publishers): a
short-lived GitHub Actions OIDC token, no stored npm token, and automatic
provenance. It runs only in the `npm` environment and only when the repository
variable `NPM_TRUSTED_PUBLISHING` is `enabled`; otherwise the release still
uploads its archives and the workflow reports the npm publish as skipped.

npm cannot attach a trusted publisher to a package that does not exist, an
unused trusted-publisher configuration expires 48 hours after it is created,
and each one must explicitly allow `npm publish`. A maintainer therefore
bootstraps once:

1. Use an npm account with two-factor authentication that can publish to the
   existing `@nowledge` organization, and npm CLI 11.15.0 or later.
2. On a macOS arm64 machine at the release tag, build the library and publish
   the first version by hand (a dry run omits `--publish`):

   ```sh
   bazel build --config=prebuilt //bindings/ffi:hawdb_ffi
   npm login
   scripts/npm/publish-local.sh <version> \
     "$(bazel cquery --config=prebuilt --output=files //bindings/ffi:hawdb_ffi 2>/dev/null)" --publish
   ```

   This publishes `@nowledge/hawdb-ffi-darwin-arm64@<version>`, a
   `@nowledge/hawdb-bun@<version>` whose optional dependencies list only that
   package, and every other platform package as a library-free
   `0.0.0-placeholder.0` under the `placeholder` dist-tag, which only creates
   the name.
3. Shortly before the next release (configurations expire after 48 unused
   hours), attach the trusted publisher to all six packages:

   ```sh
   for package in hawdb-bun hawdb-ffi-darwin-arm64 hawdb-ffi-darwin-x64 \
       hawdb-ffi-linux-x64-gnu hawdb-ffi-linux-arm64-gnu hawdb-ffi-win32-x64-msvc; do
     npm trust github "@nowledge/$package" --repo nowledge-co/hawdb \
       --file native-release.yml --env npm --allow-publish --yes
     sleep 2
   done
   ```

   or, on each package's npmjs.com **Settings → Trusted Publisher → GitHub
   Actions** page: organization `nowledge-co`, repository `hawdb`, workflow
   `native-release.yml`, environment `npm`, with `npm publish` allowed.
4. Restrict the GitHub `npm` environment to the `main` branch and `v*` tags,
   as for `pypi`, and set the repository variable
   `NPM_TRUSTED_PUBLISHING=enabled`.
5. Publish the release. If its npm job ran before the configuration existed,
   rerun the workflow with `workflow_dispatch` and the release tag.

After the first successful trusted publish, consider setting each package's
**Publishing access** to require two-factor authentication and disallow
tokens, so only the workflow can publish.
