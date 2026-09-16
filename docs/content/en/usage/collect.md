---
title: "Collect historical videos by hashtag"
weight: 5
---

# Collect historical videos by hashtag

```text
/collect @chat --tag "#tutorial"
```

Requires the existing UserBot integration to be enabled and logged in. That account must have access to the group/channel and its history. A bot token alone cannot read group history. Known chat IDs (use `/syncpeers` when necessary) and `https://t.me/username` are accepted; invite and message links are not.

The command runs without confirmation in both silent and normal mode. It reuses the user's default storage and directory, filename settings and enabled storage rules. If no default storage exists, configure it using `/storage`.

- `--type video`: defaults to video; currently the only supported type, including videos sent as documents.
- `--storage name`: overrides the default storage. Reuses the default directory only if it belongs to the selected storage.

`CHOSEN` means the selected storage. `NEW-FOR-ALBUM` uses the oldest video filename in the album as the folder name. Storage base paths still apply. No new configuration fields are introduced.

History is scanned newest first in pages, rather than depending on the Telegram search index. Full hashtags are matched case-insensitively: `#tutorial` does not match `#tutorials`. A caption on any member of a consecutive media album selects its videos, even across page boundaries. Independent nearby text messages do not label other messages. New messages after the initial history boundary are left for the next run. Within an album videos are submitted oldest first; concurrent completion order may differ.

The final destination is checked before downloading and again before uploading. Existing names are skipped without downloading, uploading, overwriting, or auto-renaming. Concurrent collection tasks coordinate active destination paths. This is not an atomic transaction with external uploaders. Different content with the same name is still skipped; renamed copies are not content-deduplicated.

Reliable checks currently support **AList/OpenList and local storage**. Unsupported destinations (including rule redirects), permission errors, timeouts and backend failures are errors, never assumed absence. Checks are retried a bounded number of times; unsuccessful items are reported as failures.

Each task uses one existing queue worker as coordinator and bounded concurrent file workers as described below. There are no new status databases, cursor files, manifests or per-message histories. Ordinary download temporary files retain their existing cleanup on completion, error and cancellation. Existing logs, OS swap and Telegram session handling remain separate from collection state.

Progress respects the existing update interval. Cancel using the button or `/cancel taskID`. Restarting the bot does not resume a collection: resend the command to scan again and skip names currently present remotely. Changing filename settings, rules or paths changes duplicate detection. No HTTP API creation is supported, since collection requires Telegram user permissions and defaults.

## Concurrent file progress

The existing `workers` setting also limits concurrent files within a collection. With `workers=2`, one collection can process two videos concurrently; all collections share two file slots. Ordinary save tasks retain their existing queue and do not share this collection-specific file limit. Scanning applies backpressure instead of accumulating unbounded jobs. Only a page, an album, bounded active items and their albums, counters and the latest error are held in memory. No new configuration or persistent state is introduced.

The same throttled status message shows individual filenames, phases, progress bars, byte counts and phase-average speeds (up to four visible rows, with a count for additional active items). Upload speed measures Bot-to-storage-endpoint transfer, not OpenList-to-115 network throughput. After transfer reaches 100%, the item waits for storage completion, which may include the cloud upload. It counts as saved only when storage returns success. Retries reset upload progress; finished rows are removed. Cancellation waits for active file cleanup before final reporting.

Concurrent same-name candidates are serialized by destination path; the first lock holder wins, so scan order does not guarantee which same-name source is kept.
