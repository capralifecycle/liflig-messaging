package no.liflig.messaging.awssdk.testutils

import java.io.InputStream

/**
 * Reads the file at the given path under the resources directory, and passes the [InputStream] of
 * the file to the given lambda.
 *
 * For example, if you want to use a file at path `src/main/resources/example.txt`, you would call
 * `useResource("example.txt") { file -> /* Use file here */ }`.
 *
 * @param path Relative path from the resources directory.
 * @throws IllegalArgumentException If no resource file was found at the given path.
 */
internal inline fun <ReturnT> useResourceFile(
    path: String,
    block: (InputStream) -> ReturnT,
): ReturnT {
  // Paths to getResourceAsStream should start with /, otherwise they're relative to the calling
  // package
  val fixedPath = if (!path.startsWith('/')) "/$path" else path
  val resource =
      ResourceLoader.javaClass.getResourceAsStream(fixedPath)
          ?: throw IllegalArgumentException("Failed to find resource at path '${path}'")
  // use ensures that the resource is closed correctly
  return resource.use(block)
}

/**
 * Reads the content of the file at the given path under the resources directory.
 *
 * For example, if you want to read a file at `src/main/resources/example.txt`, you would call
 * `readResourceFile("example.txt")`
 *
 * @param path Relative path from the resources directory.
 * @throws IllegalArgumentException If no resource file was found at the given path.
 */
internal fun readResourceFile(path: String): String {
  useResourceFile(path) { resource ->
    return resource.reader().readText()
  }
}

/**
 * Every class instance has a [Class.getResourceAsStream] method, but in Kotlin we may not always be
 * in the scope of a class instance. We use this object to get access to the resource methods from
 * anywhere.
 *
 * NB: This must be public in order for getResourceAsStream to work.
 */
internal object ResourceLoader
