import {
  copyFile,
  cp,
  mkdir,
  readdir,
  readFile,
  rename,
  rm,
  stat,
  writeFile,
  DOFS,
} from "./fs.js";

export { DOFS };

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      if (url.pathname === "/test") {
        return new Response(await runTests(), {
          headers: { "Content-Type": "text/plain;charset=utf8" },
        });
      }

      return new Response(
        "FS Test Worker\n\nVisit /test to run filesystem tests",
        {
          headers: { "Content-Type": "text/plain" },
        }
      );
    } catch (error) {
      return new Response(`Error: ${error.message}\n${error.stack}`, {
        status: 500,
        headers: { "Content-Type": "text/plain" },
      });
    }
  },
};

async function runTests() {
  const results = [];

  function log(message) {
    results.push(message);
    console.log(message);
  }

  try {
    log("🧪 Starting filesystem tests...\n");

    // Test 1: Create directories
    log("Test 1: Creating directories");
    await mkdir("/Users/testuser/documents", { recursive: true });
    await mkdir("/Users/testuser/projects/myapp", { recursive: true });
    await mkdir("/tmp", { recursive: true });
    log("✅ Directories created successfully\n");

    // Test 2: Write files
    log("Test 2: Writing files");
    await writeFile("/Users/testuser/documents/readme.txt", "Hello, World!");
    await writeFile(
      "/Users/testuser/projects/myapp/package.json",
      JSON.stringify(
        {
          name: "myapp",
          version: "1.0.0",
        },
        null,
        2
      )
    );
    await writeFile("/tmp/temp.log", "Temporary file content");
    log("✅ Files written successfully\n");

    // Test 3: Read files
    log("Test 3: Reading files");
    const readme = await readFile(
      "/Users/testuser/documents/readme.txt",
      "utf8"
    );
    log(`readme.txt content: "${readme}"`);

    const packageJson = await readFile(
      "/Users/testuser/projects/myapp/package.json",
      "utf8"
    );
    const parsed = JSON.parse(packageJson);
    log(`package.json name: "${parsed.name}"`);

    const tempLog = await readFile("/tmp/temp.log", "utf8");
    log(`temp.log content: "${tempLog}"`);
    log("✅ Files read successfully\n");

    // Test 4: List directories
    log("Test 4: Listing directories");
    const userFiles = await readdir("/Users/testuser");
    log(`/Users/testuser contents: ${userFiles.join(", ")}`);

    const docsFiles = await readdir("/Users/testuser/documents");
    log(`/Users/testuser/documents contents: ${docsFiles.join(", ")}`);

    const projectFiles = await readdir("/Users/testuser/projects");
    log(`/Users/testuser/projects contents: ${projectFiles.join(", ")}`);
    log("✅ Directory listings successful\n");

    // Test 5: File stats
    log("Test 5: Getting file stats");
    const readmeStats = await stat("/Users/testuser/documents/readme.txt");
    log(
      `readme.txt - isFile: ${readmeStats.isFile}, size: ${readmeStats.size} bytes`
    );

    const docsStats = await stat("/Users/testuser/documents");
    log(`documents - isDirectory: ${docsStats.isDirectory}`);
    log("✅ File stats retrieved successfully\n");

    // Test 6: Copy files
    log("Test 6: Copying files");
    await copyFile(
      "/Users/testuser/documents/readme.txt",
      "/tmp/readme-copy.txt"
    );
    const copiedContent = await readFile("/tmp/readme-copy.txt", "utf8");
    log(`Copied file content: "${copiedContent}"`);
    log("✅ File copied successfully\n");

    // Test 7: Copy directory (cross-instance)
    log("Test 7: Copying directory across instances");
    await cp("/Users/testuser/documents", "/tmp/documents-backup", {
      recursive: true,
    });
    const backupFiles = await readdir("/tmp/documents-backup");
    log(`Backup directory contents: ${backupFiles.join(", ")}`);
    log("✅ Directory copied successfully\n");

    // Test 8: Rename files
    log("Test 8: Renaming files");
    await rename("/tmp/temp.log", "/tmp/renamed.log");
    const renamedFiles = await readdir("/tmp");
    log(`/tmp after rename: ${renamedFiles.join(", ")}`);
    log("✅ File renamed successfully\n");

    // Test 9: Binary file handling
    log("Test 9: Binary file handling");
    const binaryData = new Uint8Array([
      0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
    ]); // PNG header
    await writeFile("/Users/testuser/documents/test.png", binaryData);

    // Read without encoding to get ArrayBuffer
    const readBinary = await readFile("/Users/testuser/documents/test.png");
    const readArray = new Uint8Array(readBinary);
    log(
      `Binary file size: ${readArray.length} bytes, first byte: 0x${readArray[0]
        .toString(16)
        .padStart(2, "0")}`
    );
    log("✅ Binary file handling successful\n");

    // Test 10: Error handling
    log("Test 10: Error handling");
    try {
      await readFile("/nonexistent/file.txt");
      log("❌ Should have thrown an error");
    } catch (error) {
      log(`✅ Correctly threw error: ${error.message}`);
    }

    try {
      await mkdir("/Users/testuser/documents/readme.txt/invalid");
      log("❌ Should have thrown an error");
    } catch (error) {
      log(`✅ Correctly threw error: ${error.message}`);
    }
    log("");

    // Test 11: Remove files and directories
    log("Test 11: Cleaning up (removing files)");
    await rm("/tmp/readme-copy.txt");
    await rm("/tmp/documents-backup", { recursive: true });
    await rm("/tmp/renamed.log");
    await rm("/Users/testuser/projects", { recursive: true });

    const finalUserFiles = await readdir("/Users/testuser");
    log(`Final /Users/testuser contents: ${finalUserFiles.join(", ")}`);
    log("✅ Cleanup successful\n");

    // Test 12: Cross-instance operations
    log("Test 12: Cross-instance operations test");

    // First ensure parent directories exist
    await mkdir("/Users/alice", { recursive: true });
    await mkdir("/Users/bob", { recursive: true });

    await writeFile("/Users/alice/data.txt", "Alice data");
    await writeFile("/Users/bob/data.txt", "Bob data");

    // This should use different DO instances
    const aliceData = await readFile("/Users/alice/data.txt", "utf8");
    const bobData = await readFile("/Users/bob/data.txt", "utf8");
    log(`Alice's data: "${aliceData}"`);
    log(`Bob's data: "${bobData}"`);

    // Cross-instance copy
    await copyFile("/Users/alice/data.txt", "/Users/bob/alice-data.txt");
    const crossCopy = await readFile("/Users/bob/alice-data.txt", "utf8");
    log(`Cross-instance copy result: "${crossCopy}"`);
    log("✅ Cross-instance operations successful\n");

    log("🎉 All tests passed!");
  } catch (error) {
    log(`❌ Test failed: ${error.message}`);
    log(`Stack trace: ${error.stack}`);
  }

  return results.join("\n");
}
