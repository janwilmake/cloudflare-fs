[![](https://badge.forgithub.com/janwilmake/cloudflare-fs)](https://uithub.com/janwilmake/cloudflare-fs) [![](https://badge.xymake.com/janwilmake/status/1946939223673544864)](https://x.com/janwilmake/status/1946939223673544864) [![](https://b.letmeprompt.com)](https://letmeprompt.com?q=https://uithub.com/janwilmake/cloudflare-fs)

This package implements an opinionated durable object powered file-system that aims to replicate the exact node `fs/promises` api and make it available in workers.

```
npm i cloudflare-fs
```

Usage:

```js
// worker.js
import { writeFile, DOFS } from "cloudflare-fs";
export { DOFS };
export default {
  fetch: async (request) => {
    await writeFile("/latest-request.txt", request.url, "utf8");
    return new Response("written!");
  },
};
```

Add to your `wrangler.toml`

```toml
[[durable_objects.bindings]]
name = "DOFS"
class_name = "DOFS"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["DOFS"]
```

# Limitations compared to Node.js fs:

- **No streaming APIs** - missing `createReadStream`, `createWriteStream`
- **No sync versions** - missing `readFileSync`, `writeFileSync`, etc.
- **No watch functionality** - missing `watch`, `watchFile`
- **No advanced features** - missing `link`, `symlink`, `readlink`, `chmod`, `chown`
- **No file descriptors** - missing `open`, `close`, `read`, `write` with fd
- **Cross-instance operations** are simplified and may be slower
- **No proper error codes** - Node.js fs uses specific error codes like ENOENT, EISDIR
- **Limited encoding support** - only basic TextEncoder/TextDecoder
- **Limited max filesize** - capped at the max rowsize of 2MB
- **Limited max total disk size** - capped at 10GB per disk\*
- Every fs request does a round-trip to the DO! This can make these operations rather slow if you have lots of them if the DO is not in the same place as the worker. I wonder though, how fast it will be if ran from a DO in the same spot.

# How do disks work?

- every username in paths starting with `/Users/{username}` becomes its own disk (DO)
- anything else goes to the 'default' disk.
