# Overview
The _*data-tools*_ library aims to provide generic and flexible data parsers to manage the loading of unknown
data sets into a few popular back end data stores.

There are some enforced opinions on the definition of a "generic" document when referring to unknown
data. Ideally data structures should be unaltered. Odd output formatting should be addressed or accounted
for while parsing in order to make sure the data being parsed is as portable as possible. 

#### For example:

Given a json string such as the following -

```json
{
  "owner.account_id_type": "appUserId",
  "owner.phone_number": "(222) 222-2222",
  "property.address": "4242 Jupiter Way",
  "property.city": "Jupter",
  "property.price": 6900042,
  "property.state": "FL",
  "property.zip_code": "42424",
  "property.pid": "54321",
  "recipient.account_id": 3141592,
  "recipient.account_id_type": "appUserId",
  "recipient.email_address": "recipient@email.example",
  "recipient.name": "Recipient Email User",
  "recipient.phone_number": "(444) 444-4444",
  "request_id": "987654",
  "sender.email_address": "sender@email.example",
  "sender.name": "Sender Email User",
  "sender.phone_number": "(202) 222-2222",
  "sender.id": "UUID"
}
```

Several of these keys use dot notation for indexing nested objects. This technically conforms to what's allowed within
the standard protocol definition but it can cause issues depending how how it's used.

The `JsonParser` will handle this by combining nested elements that are found using dot notation into valid json objects.

The previous example parses into the following:

```json
{
  "owner": { 
    "account_id_type": "appUserId",
    "phone_number": "(222) 222-2222"
  },
  "property": {
    "address": "4242 Jupiter Way",
    "city": "Jupter",
    "price": 6900042,
    "state": "FL",
    "zip_code": "42424",
    "pid": "54321"
  },
  "recipient": {
    "account_id": 3141592
    "account_id_type": "appUserId",
    "email_address": "recipient@email.example",
    "name": "Recipient Email User",
    "phone_number": "(444) 444-4444"
  }
  "request_id": "987654",
  "sender": {
    "email_address": "sender@email.example",
    "name": "Sender Email User",
    "phone_number": "(202) 222-2222",
    "id": "UUID"
  }
}
```

This is a much easier format to manipulate and use in a generic way as it conforms to the base standard and can be easily manipulated for loading. 

More data loading schemes are added as needed. 

### Testing

A useful dataset is found here https://www.kaggle.com/rounakbanik/the-movies-dataset).

Download the [Test data](https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip)
for use with the `CsvFile` parser to determine how field separation and pattern matching work

