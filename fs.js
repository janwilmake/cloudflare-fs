//@ts-check
/// <reference lib="esnext" />
/// <reference types="@cloudflare/workers-types" />
import { DurableObject, env } from "cloudflare:workers";

/**
 * @typedef Env
 * @property {DurableObjectNamespace<DOFS>} DOFS
 */

/**
 * Represents a file or directory entry in the filesystem
 * @typedef {Object} File
 * @property {string} path - The full path of the file/directory (PRIMARY KEY)
 * @property {string|null} parent_path - The path of the parent directory
 * @property {string} name - The name of the file/directory (NOT NULL)
 * @property {'file'|'directory'} type - The type of entry (NOT NULL, must be 'file' or 'directory')
 * @property {Blob|null} content - The binary content of the file (null for directories)
 * @property {number} size - The size of the file in bytes (default: 0)
 * @property {number} mode - The file permissions/mode (default: 33188 for regular files)
 * @property {number} uid - The user ID of the file owner (default: 0)
 * @property {number} gid - The group ID of the file owner (default: 0)
 * @property {number} mtime - The modification time as Unix timestamp (default: current time)
 * @property {number} ctime - The creation time as Unix timestamp (default: current time)
 * @property {number} atime - The access time as Unix timestamp (default: current time)
 */

/**
 * @typedef {Object} Stats
 * @property {boolean} isFile - Returns true if the item is a file
 * @property {boolean} isDirectory - Returns true if the item is a directory
 * @property {boolean} isSymbolicLink - Returns true if the item is a symbolic link
 * @property {number} size - Size of the file in bytes
 * @property {Date} mtime - Modified time
 * @property {Date} ctime - Created time
 * @property {Date} atime - Accessed time
 * @property {number} mode - File mode/permissions
 * @property {number} uid - User ID of owner
 * @property {number} gid - Group ID of owner
 */

/**
 * @typedef {Object} CopyOptions
 * @property {boolean} [force] - Overwrite existing file or directory
 * @property {boolean} [preserveTimestamps] - Preserve timestamps
 * @property {boolean} [recursive] - Copy directories recursively
 * @property {function} [filter] - Function to filter copied files
 */

/**
 * @typedef {Object} WriteFileOptions
 * @property {string} [encoding='utf8'] - Character encoding
 * @property {number} [mode=0o666] - File mode
 * @property {string} [flag='w'] - File system flag
 */

/**
 * @typedef {Object} ReadFileOptions
 * @property {string} [encoding] - Character encoding (if not specified, returns Buffer)
 * @property {string} [flag='r'] - File system flag
 */

/**
 * @typedef {Object} MkdirOptions
 * @property {boolean} [recursive=false] - Create parent directories if they don't exist
 * @property {number} [mode=0o777] - Directory mode
 */

/**
 * @typedef {Object} RmOptions
 * @property {boolean} [force=false] - Ignore nonexistent files
 * @property {boolean} [recursive=false] - Remove directories recursively
 * @property {number} [maxRetries=0] - Maximum number of retry attempts
 * @property {number} [retryDelay=100] - Delay between retries in ms
 */

// Global env reference
/**
 * Set the environment for the fs module
 * @type {Env} env - The environment object
 */
let globalEnv = env;

/**
 * Get DO instance name from path
 * @param {string} path - File path
 * @returns {string} - DO instance name
 */
function getInstanceName(path) {
  const userMatch = path.match(/^\/Users\/([^\/]+)/);
  return userMatch ? userMatch[1] : "default";
}

/**
 * Get DOFS instance for path
 * @param {string} path - File path
 * @returns {DurableObjectStub<DOFS>} - DOFS instance
 */
function getInstance(path) {
  if (!globalEnv) {
    throw new Error("Environment not set. Call setEnv(env) first.");
  }
  const name = getInstanceName(path);
  return globalEnv.DOFS.get(globalEnv.DOFS.idFromName(name));
}

/**
 * Ensure parent directories exist across instances
 * @param {string} path - File path
 */
async function ensureParentExists(path) {
  const normalized = path.replace(/\/+$/, "").replace(/\/+/g, "/");
  if (normalized === "/" || !normalized.includes("/")) return;

  const lastSlash = normalized.lastIndexOf("/");
  const parentPath = lastSlash <= 0 ? "/" : normalized.substring(0, lastSlash);

  if (parentPath === "/") return;

  try {
    await stat(parentPath);
  } catch {
    // Parent doesn't exist, create it
    await mkdir(parentPath, { recursive: true });
  }
}

/**
 * Copy a file from source to destination
 * @param {string} src - Source file path
 * @param {string} dest - Destination file path
 * @param {number} [mode] - Optional mode specifying behavior
 * @returns {Promise<void>}
 */
export async function copyFile(src, dest, mode = 0) {
  const srcInstance = getInstance(src);
  const destInstance = getInstance(dest);

  if (srcInstance === destInstance) {
    await srcInstance.copyFile(src, dest, mode);
  } else {
    // Ensure parent directory exists in destination instance
    await ensureParentExists(dest);

    const content = await srcInstance.readFileBuffer(src);
    const stats = await srcInstance.stat(src);
    await destInstance.writeFileBuffer(dest, content, { mode: stats.mode });
  }
}

/**
 * Copy files and directories
 * @param {string} src - Source path
 * @param {string} dest - Destination path
 * @param {CopyOptions} [options] - Copy options
 * @returns {Promise<void>}
 */
export async function cp(src, dest, options = {}) {
  const srcInstance = getInstance(src);
  const destInstance = getInstance(dest);

  if (srcInstance === destInstance) {
    await srcInstance.cp(src, dest, options);
  } else {
    // Cross-instance copy - simplified implementation
    const stats = await srcInstance.stat(src);
    if (stats.isDirectory) {
      if (!options.recursive) {
        throw new Error("Cannot copy directory without recursive option");
      }
      await mkdir(dest, { recursive: true });
      const entries = await srcInstance.readdir(src);
      for (const entry of entries) {
        await cp(`${src}/${entry}`, `${dest}/${entry}`, options);
      }
    } else {
      await ensureParentExists(dest);
      const content = await srcInstance.readFileBuffer(src);
      await destInstance.writeFileBuffer(dest, content, { mode: stats.mode });
    }
  }
}

/**
 * Create a directory
 * @param {string} path - Directory path to create
 * @param {MkdirOptions} [options] - Directory creation options
 * @returns {Promise<string|undefined>} - Returns path of first directory created (when recursive)
 */
export async function mkdir(path, options = {}) {
  const instance = getInstance(path);
  return await instance.mkdir(path, options);
}

/**
 * Read directory contents
 * @param {string} path - Directory path to read
 * @param {Object} [options] - Read options
 * @param {string} [options.encoding='utf8'] - Character encoding for filenames
 * @param {boolean} [options.withFileTypes=false] - Return Dirent objects instead of strings
 * @returns {Promise<string[]|Object[]>} - Array of filenames or Dirent objects
 */
export async function readdir(path, options = {}) {
  const instance = getInstance(path);
  return await instance.readdir(path, options);
}

/**
 * Read file contents
 * @param {string} path - File path to read
 * @param {ReadFileOptions|string} [options] - Read options or encoding string
 * @returns {Promise<Buffer|string>} - File contents as Buffer or string
 */
export async function readFile(path, options) {
  const instance = getInstance(path);
  return await instance.readFile(path, options);
}

/**
 * Rename/move a file or directory
 * @param {string} oldPath - Current path
 * @param {string} newPath - New path
 * @returns {Promise<void>}
 */
export async function rename(oldPath, newPath) {
  const oldInstance = getInstance(oldPath);
  const newInstance = getInstance(newPath);

  if (oldInstance === newInstance) {
    await oldInstance.rename(oldPath, newPath);
  } else {
    // Cross-instance move
    await cp(oldPath, newPath, { recursive: true });
    await rm(oldPath, { recursive: true });
  }
}

/**
 * Remove files and directories
 * @param {string} path - Path to remove
 * @param {RmOptions} [options] - Remove options
 * @returns {Promise<void>}
 */
export async function rm(path, options = {}) {
  const instance = getInstance(path);
  await instance.rm(path, options);
}

/**
 * Get file/directory statistics
 * @param {string} path - Path to stat
 * @param {Object} [options] - Stat options
 * @param {boolean} [options.bigint=false] - Return BigInt values for numeric properties
 * @returns {Promise<Stats>} - File statistics object
 */
export async function stat(path, options = {}) {
  const instance = getInstance(path);
  return await instance.stat(path, options);
}

/**
 * Write data to a file
 * @param {string} file - File path to write
 * @param {string|ArrayBuffer|Uint8Array} data - Data to write
 * @param {WriteFileOptions|string} [options] - Write options or encoding string
 * @returns {Promise<void>}
 */
export async function writeFile(file, data, options) {
  const instance = getInstance(file);
  await instance.writeFile(file, data, options);
}

export class DOFS extends DurableObject {
  /** @param {DurableObjectState} state @param {Env} env */
  constructor(state, env) {
    super(state, env);
    this.sql = state.storage.sql;
    this.env = env;
    this.initTables();
  }

  initTables() {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS files (
        path TEXT PRIMARY KEY,
        parent_path TEXT,
        name TEXT NOT NULL,
        type TEXT NOT NULL CHECK (type IN ('file', 'directory')),
        content BLOB,
        size INTEGER NOT NULL DEFAULT 0,
        mode INTEGER NOT NULL DEFAULT 33188,
        uid INTEGER NOT NULL DEFAULT 0,
        gid INTEGER NOT NULL DEFAULT 0,
        mtime INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
        ctime INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
        atime INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
      )
    `);

    this.sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_parent_path ON files(parent_path)`
    );
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_name ON files(name)`);
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_type ON files(type)`);
  }

  /**
   * Normalize path by removing trailing slashes and resolving relative parts
   * @param {string} path - Path to normalize
   * @returns {string} - Normalized path
   */
  normalizePath(path) {
    if (path === "/") return "/";
    return path.replace(/\/+$/, "").replace(/\/+/g, "/");
  }

  /**
   * Get parent directory path
   * @param {string} path - File path
   * @returns {string} - Parent directory path
   */
  getParentPath(path) {
    const normalized = this.normalizePath(path);
    if (normalized === "/") return null;
    const lastSlash = normalized.lastIndexOf("/");
    return lastSlash <= 0 ? "/" : normalized.substring(0, lastSlash);
  }

  /**
   * Get filename from path
   * @param {string} path - File path
   * @returns {string} - Filename
   */
  getFileName(path) {
    const normalized = this.normalizePath(path);
    if (normalized === "/") return "";
    const lastSlash = normalized.lastIndexOf("/");
    return normalized.substring(lastSlash + 1);
  }

  /**
   * Copy a file
   * @param {string} src - Source path
   * @param {string} dest - Destination path
   * @param {number} mode - Copy mode
   */
  async copyFile(src, dest, mode) {
    const srcFile = this.sql
      .exec("SELECT * FROM files WHERE path = ?", src)
      .toArray()[0];
    if (!srcFile || srcFile.type !== "file") {
      throw new Error("Source file not found");
    }

    const destParent = this.getParentPath(dest);
    if (destParent && destParent !== "/") {
      const parent = this.sql
        .exec("SELECT * FROM files WHERE path = ?", destParent)
        .toArray()[0];
      if (!parent || parent.type !== "directory") {
        throw new Error("Destination directory does not exist");
      }
    }

    const now = Math.floor(Date.now() / 1000);
    this.sql.exec(
      `
      INSERT OR REPLACE INTO files 
      (path, parent_path, name, type, content, size, mode, uid, gid, mtime, ctime, atime)
      VALUES (?, ?, ?, 'file', ?, ?, ?, ?, ?, ?, ?, ?)
    `,
      dest,
      destParent,
      this.getFileName(dest),
      srcFile.content,
      srcFile.size,
      srcFile.mode,
      srcFile.uid,
      srcFile.gid,
      now,
      now,
      now
    );
  }

  /**
   * Copy files and directories recursively
   * @param {string} src - Source path
   * @param {string} dest - Destination path
   * @param {CopyOptions} options - Copy options
   */
  async cp(src, dest, options = {}) {
    const srcFile = this.sql
      .exec("SELECT * FROM files WHERE path = ?", src)
      .toArray()[0];
    if (!srcFile) {
      throw new Error("Source does not exist");
    }

    if (srcFile.type === "file") {
      await this.copyFile(src, dest, 0);
    } else if (srcFile.type === "directory") {
      if (!options.recursive) {
        throw new Error("Cannot copy directory without recursive option");
      }

      // Create destination directory
      await this.mkdir(dest, { recursive: true });

      // Copy all children
      const children = this.sql
        .exec("SELECT * FROM files WHERE parent_path = ?", src)
        .toArray();
      for (const child of children) {
        const childSrc = child.path;
        const childDest = `${dest}/${child.name}`;
        await this.cp(childSrc, childDest, options);
      }
    }
  }

  /**
   * Create directory
   * @param {string} path - Directory path
   * @param {MkdirOptions} options - Options
   * @returns {Promise<string|undefined>} - First created directory path
   */
  async mkdir(path, options = {}) {
    const normalizedPath = this.normalizePath(path);

    // Check if already exists
    const existing = this.sql
      .exec("SELECT * FROM files WHERE path = ?", normalizedPath)
      .toArray()[0];
    if (existing) {
      if (existing.type === "directory") {
        return undefined; // Already exists
      } else {
        throw new Error("File exists and is not a directory");
      }
    }

    const parentPath = this.getParentPath(normalizedPath);
    let firstCreated = undefined;

    if (parentPath && parentPath !== "/") {
      const parent = this.sql
        .exec("SELECT * FROM files WHERE path = ?", parentPath)
        .toArray()[0];
      if (!parent) {
        if (options.recursive) {
          firstCreated = await this.mkdir(parentPath, options);
        } else {
          throw new Error("Parent directory does not exist");
        }
      } else if (parent.type !== "directory") {
        throw new Error("Parent is not a directory");
      }
    }

    const now = Math.floor(Date.now() / 1000);
    const mode = options.mode || 0o777;

    this.sql.exec(
      `
      INSERT INTO files 
      (path, parent_path, name, type, size, mode, uid, gid, mtime, ctime, atime)
      VALUES (?, ?, ?, 'directory', 0, ?, 0, 0, ?, ?, ?)
    `,
      normalizedPath,
      parentPath,
      this.getFileName(normalizedPath),
      mode,
      now,
      now,
      now
    );

    return firstCreated || normalizedPath;
  }

  /**
   * Read directory contents
   * @param {string} path - Directory path
   * @param {Object} options - Read options
   * @returns {Promise<string[]|Object[]>} - Directory entries
   */
  async readdir(path, options = {}) {
    const normalizedPath = this.normalizePath(path);
    const dir = this.sql
      .exec("SELECT * FROM files WHERE path = ?", normalizedPath)
      .toArray()[0];

    if (!dir) {
      throw new Error("Directory does not exist");
    }
    if (dir.type !== "directory") {
      throw new Error("Not a directory");
    }

    /**
     * @type {File[]}
     */
    const entries = this.sql
      .exec(
        "SELECT * FROM files WHERE parent_path = ? ORDER BY name",
        normalizedPath
      )
      .toArray();

    if (options.withFileTypes) {
      return entries.map((entry) => ({
        name: entry.name,
        isFile: () => entry.type === "file",
        isDirectory: () => entry.type === "directory",
        isSymbolicLink: () => false,
      }));
    }

    return entries.map((entry) => entry.name);
  }

  /**
   * Read file contents as buffer
   * @param {string} path - File path
   * @returns {Promise<ArrayBuffer>} - File contents
   */
  async readFileBuffer(path) {
    /** @type {File} */
    const file = this.sql
      .exec("SELECT * FROM files WHERE path = ?", path)
      .toArray()[0];

    if (!file) {
      throw new Error("File does not exist");
    }
    if (file.type !== "file") {
      throw new Error("Not a file");
    }

    // Update access time
    const now = Math.floor(Date.now() / 1000);
    this.sql.exec("UPDATE files SET atime = ? WHERE path = ?", now, path);

    return file.content || new ArrayBuffer(0);
  }

  /**
   * Read file contents
   * @param {string} path - File path
   * @param {ReadFileOptions|string} options - Read options
   * @returns {Promise<ArrayBuffer|string>} - File contents
   */
  async readFile(path, options) {
    const buffer = await this.readFileBuffer(path);

    let encoding = null;
    if (typeof options === "string") {
      encoding = options;
    } else if (options && options.encoding) {
      encoding = options.encoding;
    }

    // If no encoding specified, return ArrayBuffer (like Node.js Buffer)
    if (!encoding) {
      return buffer;
    }

    const decoder = new TextDecoder(encoding);
    return decoder.decode(buffer);
  }

  /**
   * Rename/move a file or directory
   * @param {string} oldPath - Current path
   * @param {string} newPath - New path
   */
  async rename(oldPath, newPath) {
    const normalizedOld = this.normalizePath(oldPath);
    const normalizedNew = this.normalizePath(newPath);

    const file = this.sql
      .exec("SELECT * FROM files WHERE path = ?", normalizedOld)
      .toArray()[0];
    if (!file) {
      throw new Error("Source does not exist");
    }

    const newParent = this.getParentPath(normalizedNew);
    if (newParent && newParent !== "/") {
      const parent = this.sql
        .exec("SELECT * FROM files WHERE path = ?", newParent)
        .toArray()[0];
      if (!parent || parent.type !== "directory") {
        throw new Error("Destination directory does not exist");
      }
    }

    const now = Math.floor(Date.now() / 1000);

    // Update the file itself
    this.sql.exec(
      `
      UPDATE files 
      SET path = ?, parent_path = ?, name = ?, mtime = ?
      WHERE path = ?
    `,
      normalizedNew,
      newParent,
      this.getFileName(normalizedNew),
      now,
      normalizedOld
    );

    // If it's a directory, update all children
    if (file.type === "directory") {
      /** @type {File[]} */
      const children = this.sql
        .exec("SELECT * FROM files WHERE path LIKE ?", `${normalizedOld}/%`)
        .toArray();
      for (const child of children) {
        const newChildPath = child.path.replace(normalizedOld, normalizedNew);
        const newChildParent = this.getParentPath(newChildPath);
        this.sql.exec(
          `
          UPDATE files 
          SET path = ?, parent_path = ?
          WHERE path = ?
        `,
          newChildPath,
          newChildParent,
          child.path
        );
      }
    }
  }

  /**
   * Remove files and directories
   * @param {string} path - Path to remove
   * @param {RmOptions} options - Remove options
   */
  async rm(path, options = {}) {
    const normalizedPath = this.normalizePath(path);
    const file = this.sql
      .exec("SELECT * FROM files WHERE path = ?", normalizedPath)
      .toArray()[0];

    if (!file) {
      if (options.force) {
        return;
      }
      throw new Error("File does not exist");
    }

    if (file.type === "directory") {
      if (!options.recursive) {
        /** @type {{count:number}} */
        const child = this.sql
          .exec(
            "SELECT COUNT(*) as count FROM files WHERE parent_path = ?",
            normalizedPath
          )
          .toArray()[0];
        if (child.count > 0) {
          throw new Error("Directory not empty");
        }
      } else {
        // Remove all children recursively
        this.sql.exec(
          "DELETE FROM files WHERE path LIKE ?",
          `${normalizedPath}/%`
        );
      }
    }

    // Remove the file/directory itself
    this.sql.exec("DELETE FROM files WHERE path = ?", normalizedPath);
  }

  /**
   * Get file statistics
   * @param {string} path - File path
   * @param {Object} options - Stat options
   * @returns {Promise<Stats>} - File statistics
   */
  async stat(path, options = {}) {
    const file = this.sql
      .exec("SELECT * FROM files WHERE path = ?", path)
      .toArray()[0];
    if (!file) {
      throw new Error("File does not exist");
    }

    return {
      isFile: file.type === "file",
      isDirectory: file.type === "directory",
      isSymbolicLink: false,
      size: file.size,
      mode: file.mode,
      uid: file.uid,
      gid: file.gid,
      mtime: new Date(file.mtime * 1000),
      ctime: new Date(file.ctime * 1000),
      atime: new Date(file.atime * 1000),
    };
  }

  /**
   * Write buffer to file
   * @param {string} path - File path
   * @param {ArrayBuffer|Uint8Array} data - Data to write
   * @param {Object} options - Write options
   */
  async writeFileBuffer(path, data, options = {}) {
    const normalizedPath = this.normalizePath(path);
    const parentPath = this.getParentPath(normalizedPath);

    if (parentPath && parentPath !== "/") {
      const parent = this.sql
        .exec("SELECT * FROM files WHERE path = ?", parentPath)
        .toArray()[0];
      if (!parent || parent.type !== "directory") {
        throw new Error("Parent directory does not exist");
      }
    }

    const buffer =
      data instanceof ArrayBuffer
        ? data
        : data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
    const size = buffer.byteLength;
    const mode = options.mode || 0o666;
    const now = Math.floor(Date.now() / 1000);

    const existing = this.sql
      .exec("SELECT * FROM files WHERE path = ?", normalizedPath)
      .toArray()[0];

    if (existing) {
      // Update existing file
      this.sql.exec(
        `
        UPDATE files 
        SET content = ?, size = ?, mode = ?, mtime = ?, atime = ?
        WHERE path = ?
      `,
        buffer,
        size,
        mode,
        now,
        now,
        normalizedPath
      );
    } else {
      // Create new file
      this.sql.exec(
        `
        INSERT INTO files 
        (path, parent_path, name, type, content, size, mode, uid, gid, mtime, ctime, atime)
        VALUES (?, ?, ?, 'file', ?, ?, ?, 0, 0, ?, ?, ?)
      `,
        normalizedPath,
        parentPath,
        this.getFileName(normalizedPath),
        buffer,
        size,
        mode,
        now,
        now,
        now
      );
    }
  }

  /**
   * Write data to file
   * @param {string} path - File path
   * @param {string|ArrayBuffer|Uint8Array} data - Data to write
   * @param {WriteFileOptions|string} options - Write options
   */
  async writeFile(path, data, options) {
    let buffer;
    let writeOptions = {};

    if (typeof options === "string") {
      writeOptions = { encoding: options };
    } else if (options) {
      writeOptions = options;
    }

    if (typeof data === "string") {
      const encoding = writeOptions.encoding || "utf8";
      const encoder = new TextEncoder();
      buffer = encoder.encode(data).buffer;
    } else if (data instanceof ArrayBuffer) {
      buffer = data;
    } else if (data instanceof Uint8Array) {
      buffer = data.buffer.slice(
        data.byteOffset,
        data.byteOffset + data.byteLength
      );
    } else {
      throw new Error("Unsupported data type");
    }

    await this.writeFileBuffer(path, buffer, writeOptions);
  }
}
