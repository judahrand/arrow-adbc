# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# If we are building within the repo, copy the latest adbc.h and driver source
# into src/
files_to_vendor <- c(
  "../../adbc.h",
  "../../c/driver/postgresql/postgres_util.h",
  "../../c/driver/postgresql/postgres_type.h",
  "../../c/driver/postgresql/copy/copy_common.h",
  "../../c/driver/postgresql/copy/reader.h",
  "../../c/driver/postgresql/copy/writer.h",
  "../../c/driver/postgresql/statement.h",
  "../../c/driver/postgresql/statement.cc",
  "../../c/driver/postgresql/connection.h",
  "../../c/driver/postgresql/connection.cc",
  "../../c/driver/postgresql/error.h",
  "../../c/driver/postgresql/error.cc",
  "../../c/driver/postgresql/database.h",
  "../../c/driver/postgresql/database.cc",
  "../../c/driver/postgresql/postgresql.cc",
  "../../c/driver/postgresql/result_helper.h",
  "../../c/driver/postgresql/result_helper.cc",
  "../../c/driver/common/options.h",
  "../../c/driver/common/utils.h",
  "../../c/driver/common/utils.c",
  "../../c/vendor/nanoarrow/nanoarrow.h",
  "../../c/vendor/nanoarrow/nanoarrow.hpp",
  "../../c/vendor/nanoarrow/nanoarrow.c"
)

if (all(file.exists(files_to_vendor))) {
  files_dst <- file.path("src", basename(files_to_vendor))

  n_removed <- suppressWarnings(sum(file.remove(files_dst)))
  if (n_removed > 0) {
    cat(sprintf("Removed %d previously vendored files from src/\n", n_removed))
  }

  cat(
    sprintf(
      "Vendoring files from arrow-adbc to src/:\n%s\n",
      paste("-", files_to_vendor, collapse = "\n")
    )
  )

  if (!dir.exists("src/copy")) {
    dir.create("src/copy")
  }

  if (all(file.copy(files_to_vendor, "src"))) {
    file.rename(
      c(
        "src/nanoarrow.c",
        "src/nanoarrow.h",
        "src/nanoarrow.hpp",
        "src/options.h",
        "src/utils.c",
        "src/utils.h",
        "src/copy_common.h",
        "src/reader.h",
        "src/writer.h"
      ),
      c(
        "src/nanoarrow/nanoarrow.c",
        "src/nanoarrow/nanoarrow.h",
        "src/nanoarrow/nanoarrow.hpp",
        "src/common/options.h",
        "src/common/utils.c",
        "src/common/utils.h",
        "src/copy/copy_common.h",
        "src/copy/reader.h",
        "src/copy/writer.h"
      )
    )
    cat("All files successfully copied to src/\n")
  } else {
    stop("Failed to vendor all files")
  }
}
