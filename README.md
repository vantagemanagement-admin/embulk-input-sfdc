[![Build Status](https://travis-ci.org/treasure-data/embulk-input-sfdc.svg)](https://travis-ci.org/treasure-data/embulk-input-sfdc)
[![Code Climate](https://codeclimate.com/github/treasure-data/embulk-input-sfdc/badges/gpa.svg)](https://codeclimate.com/github/treasure-data/embulk-input-sfdc)
[![Test Coverage](https://codeclimate.com/github/treasure-data/embulk-input-sfdc/badges/coverage.svg)](https://codeclimate.com/github/treasure-data/embulk-input-sfdc/coverage)

# embulk-input-sfdc

embulk-input-sfdc is the Embulk input plugin for [Salesforce](http://www.salesforce.com/).

This plugin uses Force.com REST API.

## Overview

Required Embulk version >= 0.6.12.

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: yes

## Setup

### How to obtain OAuth Client ID and Client Secret

You should create "Connected App" in SalesForce website.

First, please visit "Setup" -> "App Setup" -> "Create" -> "Apps", scroll down to "Connected Apps" and click "New" button. Please create "Connected App" its "Enable OAuth Settings" is checked and "Selected OAuth Access" is "Full access (full)".

Then, please click you connected app name in "Connected Apps" page (visit "Setup" -> "App Setup" -> "Create" -> "Apps"). It will show "Consumer Key" (as "Client ID") and Consumer Secret (as "Client Secret").

### Configuration

- **username** username for Force.com REST API (string, required)
- **password** password for Force.com REST API (string, required)
- **client_id** client id for your application (string, required)
- **client_secret** client secret for your application (string, required)
- **security_token** your security token (string, required)
- **login_url** your login URL  (string, required)
- **target** target SObject name(string, required)
- **columns** target SObject attributes. You can generate this configuration by `guess` command (array, required)

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
