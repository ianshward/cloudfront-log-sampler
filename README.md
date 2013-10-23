CloudFront Log Sampler
======================

Sample or full analysis of CloudFront logs.  This is very basic for now.

Usage
=====

*Analyze 10% of logs from specified hour*

node index.js --key XYZ --secret XYZ --bucket my-log-bucket --prefix foo/XYZ.2013-10-22-18

*Analyze all logs from specified hour*

node index.js --key XYZ --secret XYZ --bucket my-log-bucket --prefix foo/XYZ.2013-10-22-18 --full

*Output list of all requests*

node index.js --key XYZ --secret XYZ --bucket my-log-bucket --prefix foo/XYZ.2013-10-22-18 --log

Todo
====

This is very basic, useful for a specific need for now.
