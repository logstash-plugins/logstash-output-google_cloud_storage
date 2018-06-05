## 3.2.1
  - Refactoring work to add locks to file rotation and writing.
    - Fixes [#2](https://github.com/logstash-plugins/logstash-output-google_cloud_storage/issues/2) - Plugin crashes on file rotation.
    - Fixes [#19](https://github.com/logstash-plugins/logstash-output-google_cloud_storage/issues/19) - Deleted files remain in use by the system eventually filling up disk space.

## 3.2.0
  - Change uploads to use a job pool for better performance
    - Fixes [#22](https://github.com/logstash-plugins/logstash-output-google_cloud_storage/issues/22) - Refactor Job Queue Architecture
    - Fixes [#5](https://github.com/logstash-plugins/logstash-output-google_cloud_storage/issues/5) - Major Performance Issues
  - Wait for files to upload before Logstash quits
    - Fixes [#15](https://github.com/logstash-plugins/logstash-output-google_cloud_storage/issues/15) - Fails to upload files when Logstash exits

## 3.1.0
  - Add support for disabling hostname in the log file names
  - Add support for adding a UUID to the log file names

## 3.0.5
  - Docs: Set the default_codec doc attribute.

## 3.0.4
  - Fix some documentation issues

## 3.0.2
  - Docs: Fix doc formatting

## 3.0.1
  - align the dependency on mime-type and google-api-client with the `logstash-output-google_bigquery`

## 3.0.0
  - Breaking: Updated plugin to use new Java Event APIs
  - relax contraints on logstash-core-plugin-api
  - Update .travis.yml
  - Freeze google-api-client and mime-types
  - use concurrency :single

## 2.0.4
  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

## 2.0.3
  - New dependency requirements for logstash-core for the 5.0 release

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

## 0.2.0
  - Changed the Google Cloud Storage API version to v1
  - Added simple test for plugin lookup
