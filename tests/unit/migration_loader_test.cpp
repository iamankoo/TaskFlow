#include <gtest/gtest.h>

#include "taskflow/common/errors.hpp"
#include "taskflow/db/migration_runner.hpp"
#include "test_support.hpp"

namespace taskflow {
namespace {

TEST(MigrationLoaderTest, LoadsSqlFilesSortedByVersionAndIgnoresOtherFiles) {
  testing::TempDir dir;
  dir.Write("0002_add_index.sql", "CREATE INDEX i ON t(a);");
  dir.Write("0001_create_table.sql", "CREATE TABLE t(a int);");
  dir.Write("README.md", "not a migration");
  dir.Write("notes.txt", "also not a migration");

  const auto migrations = LoadMigrations(dir.path());
  ASSERT_EQ(migrations.size(), 2u);
  EXPECT_EQ(migrations[0].version, 1);
  EXPECT_EQ(migrations[0].name, "create_table");
  EXPECT_EQ(migrations[0].sql, "CREATE TABLE t(a int);");
  EXPECT_EQ(migrations[1].version, 2);
  EXPECT_EQ(migrations[1].name, "add_index");
}

TEST(MigrationLoaderTest, EmptyDirectoryYieldsNoMigrations) {
  testing::TempDir dir;
  EXPECT_TRUE(LoadMigrations(dir.path()).empty());
}

TEST(MigrationLoaderTest, MissingDirectoryIsAnError) {
  EXPECT_THROW(LoadMigrations("/nonexistent/migrations"), MigrationError);
}

TEST(MigrationLoaderTest, MalformedFilenamesAreRejected) {
  for (const char* bad : {"create_table.sql", "1_short.sql", "0001_Upper.sql", "0001-dash.sql",
                          "0001_.sql"}) {
    testing::TempDir dir;
    dir.Write(bad, "SELECT 1;");
    EXPECT_THROW(LoadMigrations(dir.path()), MigrationError) << bad;
  }
}

TEST(MigrationLoaderTest, DuplicateVersionsAreRejected) {
  testing::TempDir dir;
  dir.Write("0001_first.sql", "SELECT 1;");
  dir.Write("00001_second.sql", "SELECT 2;");  // also version 1
  EXPECT_THROW(LoadMigrations(dir.path()), MigrationError);
}

TEST(MigrationChecksumTest, ChangesWithContentButIgnoresLineEndingStyle) {
  const std::string unix_sql = "CREATE TABLE t (\n  a int\n);\n";
  const std::string windows_sql = "CREATE TABLE t (\r\n  a int\r\n);\r\n";
  EXPECT_EQ(MigrationChecksum(unix_sql), MigrationChecksum(windows_sql));
  EXPECT_NE(MigrationChecksum(unix_sql), MigrationChecksum("CREATE TABLE t (\n  b int\n);\n"));
  EXPECT_EQ(MigrationChecksum(unix_sql).size(), 16u);
}

TEST(MigrationLoaderTest, LoadedChecksumMatchesDirectChecksum) {
  testing::TempDir dir;
  dir.Write("0001_a.sql", "SELECT 1;\r\n");
  const auto migrations = LoadMigrations(dir.path());
  ASSERT_EQ(migrations.size(), 1u);
  EXPECT_EQ(migrations[0].checksum, MigrationChecksum("SELECT 1;\n"));
}

}  // namespace
}  // namespace taskflow
