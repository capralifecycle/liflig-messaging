package no.liflig.messaging.testutils

import java.io.File

internal fun readResourcesFileAsText(path: String): String {
  return File("src/test/resources/$path").readText()
}
