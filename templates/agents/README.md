# Agent workspace template (draft)

This is a separate, opt-in rootfs recipe for Codex, Claude Code and OpenCode.
The existing Alpine image and its deployment workflow are unchanged. This
definition is not a published artifact, a database row, or a ready-to-select
production image yet.

Direct package versions are pinned in Dockerfile.rootfs and template.json.
They were checked against the npm registry when this recipe was added. npm
transitive dependencies and OS packages are not fully lockfile/digest pinned;
the resulting artifact must be hashed and validated before publication.
No API keys, provider login sessions, projects or user settings are included.
The CLI wrappers persist runtime settings in the VM (Docker ENV alone would not).

## Build the disk image

From the repository root, with Docker running:

```sh
docker build --platform linux/amd64 --file Dockerfile.rootfs \
  --build-arg ALPINE=alpine:3.22 \
  --target agents-rootfs-export \
  --output type=local,dest=assets/agents .
```

This produces `assets/agents/rootfs-agents.ext4`, a 4 GiB logical ext4 image.
The build checks Node >=22 and every agent's version command. The default
Alpine 3.20 base is intentionally not upgraded for existing sandboxes.
Latest Claude packages need Node 22; Alpine runtime dependencies include
libgcc, libstdc++, and ripgrep. Prefer 4 GiB or more VM RAM for Claude workloads;
building all size variants does not imply small sizes are workload-qualified.

## Build and publish a snapshot release

The existing Go template-builder needs a Linux KVM host; Docker on a Mac cannot
validate Firecracker restores. Give a dedicated builder assets directory the
new agent disk at `rootfs/rootfs-alpine.ext4` (the current loader's filename),
alongside matching kernel, initramfs and Firecracker assets. Do not overwrite
the live Alpine worker assets. Configure the usual ROOT_DIRECTORY/ASSETS_PATH,
network, and storage settings described in ops/README.md.

```sh
BUILD_AGENTS=true BUILD_STANDARD=false BUILD_MICRO=false \
  ./template-builder
```

BUILD_AGENTS defaults off. When enabled, standard/micro builds default off too;
enabling standard and agents together is rejected. Agent publication never
changes active-release.json. Each restored size must pass the marker, Node,
Git and three agent CLI checks before the immutable release is published.

## Where artifacts live today

The configured object backend is Azure Blob Storage:

```text
https://<BLOB_STORAGE_ACCOUNT>.blob.core.windows.net/<BLOB_CONTAINER_NAME>/
  active-release.json
  releases/agents-<UTC-build-timestamp>/manifest.json
  releases/agents-<UTC-build-timestamp>/<size>/snap
  releases/agents-<UTC-build-timestamp>/<size>/mem
  releases/agents-<UTC-build-timestamp>/<size>/writable-seed.ext4
```

snap, mem and writable-seed.ext4 are gzip-compressed objects despite their
filenames. JSON manifests stay uncompressed. Workers download artifacts to
TEMPLATE_CACHE_DIR, verify checksums and check CPU/Firecracker/kernel/rootfs
compatibility before restoring them. TEMPLATE_STORE=local uses a local store
instead, useful for development.

Important: the current release format does NOT upload the base rootfs or kernel.
It pins their digests and expects matching worker-local assets. Existing rootfs
CI exports a GitHub Actions artifact and transfers it to workers over SSH.
An agent rootfs upload/download path still needs to be added if we want every
image asset fetched exclusively from object storage.

## Database metadata and remaining runtime work

template.json matches the platform's sandbox_templates metadata shape. It is
draft with artifactReleaseId=null intentionally. After a successful release,
record the actual release ID and mark ready only after worker qualification.
Do not insert a ready row just because this recipe exists.

Current worker boot catalog supports only alpine. Before image=agents can be
served, add multi-image rootfs/release resolution and advertise agents only on
qualified workers. Merely changing a database image label cannot select a new
rootfs. No worker catalog, live database, object storage or deployment was
modified by adding this recipe.

Provider authentication, workspace preparation, persistent remote agent sessions
and reconnect are separate follow-up work. This template supplies executables,
not those session features. Review each vendor's redistribution/hosting terms
before publishing the bundled image to customers.

Installation references:
- https://developers.openai.com/codex/cli/
- https://code.claude.com/docs/en/setup
- https://opencode.ai/docs/
