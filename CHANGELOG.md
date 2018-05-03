## 3.1.0
  - Add support for disabling hostname in the log file names
  - Add support for adding a UUID to the log file names

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
