---
name: gitflow-release
description: Cuts a new reload release using git flow (release start/finish) from develop, bumping Maven versions, drafting a CHANGELOG entry, building the reactor, then (after user confirmation) pushing and marking the GitHub release as latest via gh. Use when the user asks to "cut a release", "create a new release", "release version X.Y.Z", "run git flow release", or "bump the version" for the reload project.
version: 1.1.0
---

# reload git-flow release

Cuts a `reload` release with real `git flow release start/finish`, following the project's
Maven versioning convention. Never push without explicit user confirmation — see "Push" (section 4) below.

## 0. Pre-flight

1. `git fetch origin --tags`.
2. Compare `master`/`develop` against `origin/master`/`origin/develop`. If either is behind
   (`git rev-list --count <branch>..origin/<branch>` > 0) and not ahead, fast-forward it:
   `git checkout <branch> && git merge --ff-only origin/<branch>`. If a local branch has
   diverged (both ahead and behind), stop and ask the user — do not force anything.
3. Confirm git-flow is configured for this repo's actual tag convention:
   `git config --get-regexp gitflow`. It must include `gitflow.prefix.versiontag v` (existing
   tags are `v1.2.0` … `v1.7.0`, `v2.0.0`, …). If gitflow isn't configured yet:
   `git flow init -d` then `git config gitflow.prefix.versiontag v`.
4. Return to `develop`, confirm `git status` is clean.
5. Make sure the tag doesn't already exist locally or remotely (`git ls-remote --tags origin v<version>`)
   and that `gh auth status` works — section 5 needs it.

## 1. Get the version number — never guess

Find the last release: `git tag --sort=-v:refname | head -1`. Ask the user for the new version
and whether it contains breaking changes (major bump) unless they already said so. Follow semver.

## 2. `nosql` module — known, permanent wrinkle

`nosql` is an experimental module with a MongoDB-backed test suite that doesn't work
(confirmed by the user, not something to "fix"). It cannot be disabled via its `pom.xml`
(`<maven.test.skip>`) — that edit gets blocked by the auto-mode "CI bypass" classifier even
with explicit user authorization. Do not attempt to edit `nosql/pom.xml`'s test config.

Two things everyone re-deriving this hits and gets wrong:
- **`-Dtest='!com.smeup.dbnative.nosql.**'` looks right but isn't.** This repo's Surefire
  (2.12.4) doesn't support exclude-only patterns properly — combined with
  `-DfailIfNoTests=false` it silently runs **zero tests in every module**, reactor-wide. It
  looks like a clean build; it verified nothing. Don't use it.
- **`-pl '!nosql'` for the whole build breaks it.** `distribution/pom.xml` depends on the
  `nosql` artifact for its assembly jar, so excluding the module from the reactor fails
  dependency resolution.

The working sequence — build/install `nosql` without running its tests, then verify everything
else against it:

```bash
mvn -q -pl base clean install                          # real build+test of base
mvn -q -pl nosql install -Dmaven.test.skip=true         # nosql compiles+installs, its tests never run
mvn -q clean verify -pl '!nosql'                        # everything else, real tests; distribution
                                                         # resolves nosql from the local repo
```

## 3. Release steps

1. `git flow release start <version>` (from `develop`).
2. Bump the reactor version:
   `mvn versions:set -DnewVersion=<version> -DgenerateBackupPoms=false` (run at repo root — this
   updates `pom.xml` and all module poms: `base`, `distribution`, `jt400`, `manager`, `nosql`, `sql`).
   Commit as `New release <version>` (matches this repo's historical release-commit wording —
   keep it, don't switch to a conventional-commit style for this specific commit).
3. Add/update `CHANGELOG.md`: draft the new entry from
   `git log <last-tag>..develop --oneline --no-merges`. Structure per version: `Added` /
   `Changed` / `Fixed` / **`Breaking Changes`**. Flag commits as likely-breaking when they:
   replace or remove a public API signature, drop a fallback path, rename a config/env var or
   change its default, or turn a previously-tolerated case into a hard failure. Always show the
   drafted Breaking Changes section to the user for confirmation before finishing the release —
   only they know for certain which changes are actually breaking.
   Commit as `📝 docs: add CHANGELOG for <version> release`.
4. Build/verify with the sequence in section 2.
5. Finish the release: `git flow release finish -m "Release <version>" <version>`.

   **Expect a merge conflict into `master` — this is normal, not a failure.** `master` stays
   pinned at the previous release's plain version (e.g. `1.7.0`); it's never reset to a
   SNAPSHOT. The release branch bumped from `develop-SNAPSHOT`. Every module's `pom.xml` gets a
   single-line `<version>` conflict. Resolve all of them toward the release version:
   ```bash
   # verify each conflict really is just the one version line before blindly resolving:
   grep -c '<<<<<<<' pom.xml base/pom.xml distribution/pom.xml jt400/pom.xml manager/pom.xml nosql/pom.xml sql/pom.xml
   # each should print 1 — if any prints more, or a conflict is elsewhere, stop and look by hand
   git checkout --theirs pom.xml base/pom.xml distribution/pom.xml jt400/pom.xml manager/pom.xml nosql/pom.xml sql/pom.xml
   git add pom.xml base/pom.xml distribution/pom.xml jt400/pom.xml manager/pom.xml nosql/pom.xml sql/pom.xml
   git commit --no-edit
   ```
   `git flow release finish` does **not** resume after this conflict — it stops for good after
   the master merge. Finish the rest of the sequence by hand:
   ```bash
   git tag -a v<version> -m "Release <version>" master
   git checkout develop
   git merge --no-ff release/<version> -m "Merge branch 'release/<version>' into develop"
   git branch -d release/<version>
   ```
   (The merge into `develop` is a clean fast-history merge — `develop` already has everything
   the release branch has except the version bump and changelog commits, so no conflict there.)
6. Reset `develop` back to its floating snapshot version:
   `mvn versions:set -DnewVersion=develop-SNAPSHOT -DgenerateBackupPoms=false`, then commit as
   `Remove release <version> and return to SNAPSHOT` (matches this repo's historical wording).

## 4. Push — only after explicit user confirmation

Pushing is a real, outward-facing deploy: a push to `master` triggers
`.github/workflows/smeup-deploy.yml` (Nexus "releases") and `maven-central-deploy.yml` (Maven
Central); a push to `develop` also triggers both (SNAPSHOT deploy). Neither can be undone.
**Never push without an explicit yes in the current conversation** — a request to "cut a
release" is not authorization to push.

1. Show the user a summary and ask for confirmation (use AskUserQuestion):
   - version and tag (`git describe --tags master`), the commits to be pushed
     (`git log --oneline origin/master..master` and `origin/develop..develop`)
   - the exact command: `git push --atomic origin develop master v<version>`
   - the side effects: Nexus + Maven Central deploys, and the GitHub release that follows.
2. If they decline or want changes, stop and report the local state. Do not push.
3. On confirmation, push atomically so a partial failure can't leave the remote inconsistent:
   `git push --atomic origin develop master v<version>`
   If it is rejected (remote moved), stop and report — never force-push.

## 5. Publish the GitHub release and mark it latest

The deploy workflows do **not** create a GitHub Release, so do it with `gh` right after the
push succeeds (the tag must exist on the remote first):

```bash
gh release create v<version> --verify-tag --latest --title "v<version>" \
  --notes-file <notes.md>
```

- Build `<notes.md>` (in the scratchpad, not the repo) from the `CHANGELOG.md` section for
  this version, including `Breaking Changes`.
- `--latest` explicitly marks it as the latest release (GitHub would otherwise pick by date /
  semver). If the release already exists (e.g. created as a draft), use
  `gh release edit v<version> --latest` instead.
- Confirm: `gh release list --limit 3` shows `v<version>` as `Latest`, and
  `gh release view v<version> --json isLatest,tagName,url`.
- Report the release URL to the user.

## Verification

- `git log --graph --oneline --all -15` shows `release/<version>` merged into both `master` and
  `develop`, tag on `master`.
- `git describe --tags master` → `v<version>`.
- `grep -n '<version>' pom.xml` on `master` → `<version>`; on `develop` → `develop-SNAPSHOT`.
- `git status` clean; before section 4, `git log origin/master..master` / `origin/develop..develop`
  show the new local-only commits (nothing pushed yet).
- After section 5: `gh release list --limit 3` shows `v<version>` as `Latest`.

## Helper script

`scripts/commits-since-last-tag.sh` prints the non-merge commit log since the last tag, for
drafting the CHANGELOG entry in step 3.
