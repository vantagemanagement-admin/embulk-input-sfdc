[![Build Status](https://travis-ci.org/treasure-data/embulk-input-sfdc.svg?branch=master)](https://travis-ci.org/treasure-data/embulk-input-sfdc)
[![Code Climate](https://codeclimate.com/github/treasure-data/embulk-input-sfdc/badges/gpa.svg)](https://codeclimate.com/github/treasure-data/embulk-input-sfdc)
[![Test Coverage](https://codeclimate.com/github/treasure-data/embulk-input-sfdc/badges/coverage.svg)](https://codeclimate.com/github/treasure-data/embulk-input-sfdc/coverage)
[![Gem Version](https://badge.fury.io/rb/embulk-input-sfdc.svg)](https://badge.fury.io/rb/embulk-input-sfdc)

# embulk-input-sfdc

embulk-input-sfdc is the Embulk input plugin for [Salesforce](http://www.salesforce.com/).

This plugin uses Force.com REST API.

## Overview

Required Embulk version >= 0.8.7

- **Plugin type**: input
- **Resume supported**: yes (since v0.1.1)
- **Cleanup supported**: no
- **Guess supported**: yes

## Setup

### How to obtain OAuth Client ID and Client Secret

You should create "Connected App" in SalesForce website.

First, please visit "Setup" -> "App Setup" -> "Create" -> "Apps", scroll down to "Connected Apps" and click "New" button. Please create "Connected App" its "Enable OAuth Settings" is checked and "Selected OAuth Access" is "Full access (full)". Then, this plugin doesn't use "Callback URL" setting, so please specify dummy URL (e.g. http://localhost:4000) as it.

Finally, please click you connected app name in "Connected Apps" page (visit "Setup" -> "App Setup" -> "Create" -> "Apps"). It will show "Consumer Key" (as "Client ID") and Consumer Secret (as "Client Secret").

### NOTE: security token

This plugin requires your "security token", but if you forget it, please visit "My Settings" -> "Personal" -> "Reset My Security Token", and reset it. (Please attend your API applications can't work well with old security token.)

### Configuration

- **username** username for Force.com REST API (string, required)
- **password** password for Force.com REST API (string, required)
- **client_id** client id for your application (string, required)
- **client_secret** client secret for your application (string, required)
- **security_token** your security token (string, required)
- **login_url** your login URL (string, required)
- **target** target SObject name(string, required)
- **columns** target SObject attributes. You can generate this configuration by `guess` command (array, required)
- **retry_initial_wait_sec**: Wait seconds for exponential backoff initial value (integer, default: 1)
- **retry_limit**: Try to retry this times (integer, default: 5)
- **continue_from**: Only process records after this time. This option didn't filled or SOQL SELECT-clause doesn't contain `LastModifiedDate`, all fetched records are processed. (string, default: nil)
- **incremental**: If false, `continue_from` in next.yml would not be updated that means you always fetch all of data from SFDC with statically conditions. If true, `continue_from` would be updated in next.yml. As above you'll fetch fresh data since last executed only. (boolean, default: true)
- **query_type**: `query` or `queryAll`. Default value is `query`. If query_type = `queryAll`, SOQL Query will be executed with deleted items. `query` will not include results that are deleted.
- **soql**: Optional. If `soql` is set, then embulk will be loading data with given SOQL instead of generated based on `target` and `fields`.

## Example

```yaml
in:
  type: sfdc
  username: USERNAME
  password: PASSWORD
  client_id: CLIENT_ID
  client_secret: CLIENT_SECRET
  security_token: SECURITY_TOKEN
  login_url: https://your.login-url
  target: Contact
```

## Build

```
$ bundle exec rake build
```
